# レプリケーション遅延を疑ったら、犯人はトランザクションのコミット順序だった

## 障害の報告

Writer/Readレプリカ構成（Aurora MySQL）で運用中のシステムで、「登録したデータが見つからない」エラーが散発的に報告されていました。

同じ時期にlock waitやデッドロックも発生しており（[前回の記事](./final.md)で調査）、当初は「lock waitによるレプリケーション遅延」が原因だと考えていました。

---

## 1. レプリケーション遅延を疑った

前回の記事で、lock waitが引き起こすレプリカ遅延のメカニズムを検証しました。仮説はこうです。

1. 問題のTxがlock waitを引き起こす
2. 堰き止められたTxが一斉にCOMMIT → binlogに大量の変更が流入
3. Replicaの処理が追いつかず、遅延が拡大

この仮説に基づけば、AWSのReplicaLagメトリクスにスパイクが見えるはずです。

---

## 2. Auroraのメトリクスを確認した

CloudWatchでAuroraReplicaLagを確認しました。NOT FOUNDエラーが発生していた時間帯です。

**AuroraReplicaLag（平均）**: 5〜33ms。スパイクなし。

Auroraは共有ストレージ型のアーキテクチャで、WriterとReaderが同じストレージを参照します。通常のMySQL replicationのようなbinlog転送は行われず、レプリカ遅延は通常ms単位です。

メトリクスはその通りの値を示していました。**レプリケーション遅延でNOT FOUNDが数十分続くことは、Auroraでは考えにくい。**

※ AuroraReplicaLagMaximumで瞬間的なスパイク（38秒）が見えた日もありましたが、スパイクのない日でもNOT FOUNDは発生しており、全ケースを説明できませんでした。

---

## 3. ではなぜデータが見つからないのか

レプリケーション遅延ではないとすると、**Writerにデータがコミットされていない**可能性があります。後日確認するとデータは存在していたので、いずれコミットはされている。問題は**タイミング**です。

コードを追っていくと、こういう構造が見えました。

```php
// Controller
DB::beginTransaction();

// ① ApplicationRecordをINSERT（まだ未コミット）
$result = resolve(CreateApplicationRecordAction::class)->handle($data);
    // この中で:
    // - ApplicationRecordをINSERTする
    // - ProcessFollowUpJobをキューにdispatch ← ②

DB::commit(); // ③ ここでようやくコミット
```

ジョブのdispatch（②）がcommit（③）より先。**キューワーカーが②の直後にジョブを拾うと、③のcommitがまだ実行されていないため、Writerにすらデータが存在しない。**

---

## 4. 検証

MySQLのトランザクション内で未コミットのデータが別の接続から見えるか確認しました。

```
接続A: BEGIN → INSERT (id=107298) → コミットしない

接続B: SELECT WHERE id = 107298 → NULL（見えない）

接続A: COMMIT

接続B: SELECT WHERE id = 107298 → 1行（見える）
```

**未コミットのデータは、同じWriterの別接続からも不可視。** これはMySQLのトランザクション分離レベル（デフォルトのREPEATABLE READ）の正常な動作です。レプリケーション遅延とは一切関係ありません。

---

## 5. なぜ散発的に起きるのか

毎回起きるわけではありません。以下の条件が重なったときだけ発生します。

```
① beginTransaction
② INSERT（未コミット）
③ Job dispatch → キューに入る
            ↕ ← この隙間でワーカーがジョブを拾うとNOT FOUND
④ commit
```

③と④の間は通常ほんの数ms。しかし:

- キューワーカーが即座にジョブを拾える状態（アイドル中）だと、commitより先に実行される
- Controllerの処理が重い（他のDB操作やAPI呼び出しが③〜④の間にある）と、隙間が広がる
- lock waitやデッドロックが発生して③〜④の間に時間がかかると、さらに発生確率が上がる

lock waitの存在がレースコンディションの発生確率を高めていた。これが「lock waitとNOT FOUNDが同じ時間帯に起きていた」理由であり、レプリケーション遅延と誤認した原因でした。

---

## 6. 修正方法

Laravelでは`afterCommit`を指定すると、トランザクションのコミット後にジョブがdispatchされます。

```php
// 修正前
$jobs[] = (new ProcessFollowUpJob($jobId));

// 修正後
$jobs[] = (new ProcessFollowUpJob($jobId))->afterCommit();
```

または、ジョブクラスに`$afterCommit`プロパティを設定:

```php
class ProcessFollowUpJob extends BaseJob
{
    public $afterCommit = true;  // ← 追加
    // ...
}
```

これにより、dispatchのタイミングが以下のように変わります。

```
修正前: BEGIN → INSERT → dispatch → commit
修正後: BEGIN → INSERT → commit → dispatch
```

コミットが確定してからジョブが投入されるため、ワーカーは必ずコミット済のデータを読めます。

---

## 7. レプリケーション遅延ではなかった理由

| 観察された事実 | レプリケーション遅延仮説 | レースコンディション仮説 |
|-------------|:---:|:---:|
| AuroraReplicaLagにスパイクなし（3/16） | 説明できない | **説明できる** |
| NOT FOUNDがlock waitの直後に発生 | 説明できる | **説明できる**（lock waitがcommit遅延を拡大） |
| 後日確認するとデータは存在 | 説明できる | **説明できる**（commitは最終的に完了） |
| Aurora共有ストレージでms単位のlag | 矛盾する | **矛盾しない** |
| NOT FOUNDが数十分継続 | 説明できる | **説明できる**（リトライ間隔30秒 × 3回 + 関連ジョブの連鎖失敗） |

レースコンディション仮説はすべてのケースを矛盾なく説明できます。

---

## 振り返り

| 段階 | やったこと | わかったこと |
|------|-----------|------------|
| 仮説構築 | lock wait → レプリケーション遅延 → NOT FOUND | もっともらしいストーリー |
| メトリクス確認 | AuroraReplicaLagを確認 | 平均5〜33ms、スパイクなし |
| 仮説見直し | Readerの問題ではなくWriterの問題では？ | Auroraの共有ストレージでは遅延はms単位 |
| コード調査 | dispatch/commitの順序を確認 | **commitの前にdispatchしていた** |
| 検証 | 未コミットデータの可視性テスト | 別接続からは不可視。レプリケーション無関係 |

> **見えなかったのはReplicaが遅れていたからではなく、Writerがまだコミットしていなかったから。**

---

### 教訓

1. **トランザクション内でジョブをdispatchするな。** commitの後にdispatchするか、`afterCommit`を使う
2. **仮説は検証するまで信じるな。** もっともらしいストーリーに引きずられて、メトリクスの確認が後回しになった
3. **インフラの特性を理解せよ。** Auroraのレプリケーションは標準MySQLとは仕組みが違う。同じ「Writer/Reader構成」でも挙動が異なる

---

## 再現環境

commit前dispatchによるレースコンディションの検証コードを公開しています。

```bash
git clone https://github.com/TaichiFujii0326/deadlock-replication-demo.git
cd deadlock-replication-demo
make race-demo
```

```
commit前にdispatch: 10回中 10回 NOT FOUND（100%）
commit後にdispatch: 10回中  0回 NOT FOUND（0%）
```
