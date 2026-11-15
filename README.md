# hermit-csi

ローカルの LVM thin ボリュームを高速なローカルディスクとして使いつつ、S3 に**差分だけ**を継続バックアップし、
別ノードでマウントされたときは S3 から自動で復元する CSI ドライバです。Block / Filesystem の両モードに対応しています。

## 仕組み

```
chunks/<sha256>              全ボリューム共有のチャンク（中身のハッシュで重複排除）
volumes/<id>/latest.json.gz  最新マニフェスト（チャンク番号 → ハッシュ）。バックアップごとに上書き
volumes/<id>/owner.json      アタッチ先ノードと所有エポック
snapshots/<id>.json.gz       VolumeSnapshot 時点のマニフェスト（不変）
```

| 操作 | 動き |
|---|---|
| CreateVolume | 全ゼロのマニフェストを作るだけ（データ転送なし）。スナップショット／クローン元からの作成もマニフェストのコピーのみ |
| ControllerPublish | `owner.json` を新ノードに書き換えてエポックを +1。`latest.json` にもエポックを書き込み ETag を変えることで、旧ノードの条件付き書き込み（If-Match）を失敗させる（フェンシング） |
| NodeStage | ローカル LV の世代が `latest.json` と一致すればそのまま使う。違えば thin LV を作り直して S3 から並列復元し、直後に基準スナップショットを取る |
| 定期バックアップ | ボリュームごとの間隔（後述）で、thin スナップショット → `thin_delta` で前回基準との差分ブロックを取得 → 該当チャンクだけ読み、S3 に無いハッシュだけ PUT → `latest.json` を If-Match で差し替え |
| NodeUnstage | アンマウント後に最終バックアップ。LV は残す（同じノードに戻れば復元不要） |
| ControllerUnpublish | 所有を解除してエポック +1（ノード障害時の強制デタッチでも旧ノードを締め出す） |
| CreateSnapshot | デタッチ中なら `latest.json` のコピー。使用中なら所有ノードのエージェントに依頼し、その時点の差分を上げてからマニフェストを固定 |
| GC（controller） | どのマニフェストからも参照されず、猶予期間（既定 24h）を過ぎたチャンクを削除 |

全量アップロードは起きません。初回は割り当て済みブロック（`thin_dump`）だけを読み、ゼロのチャンクは送りません。
基準スナップショットを失った場合も、ローカルを読み直してハッシュが変わった分だけを上げます。

整合性について:
- Filesystem モードはスナップショット前に `fsfreeze` するので、ファイルシステムとして整合した状態になります
- Block モードはクラッシュ整合（電源断と同等）です
- **ノードが突然死んだ場合**は、最後のバックアップ以降の書き込みが失われます（RPO = そのボリュームのバックアップ間隔）

### バックアップ間隔

定期バックアップの間隔は StorageClass の `parameters.backupInterval` でボリュームごとに指定できます（Go の duration 形式、10 秒以上）。
指定が無いボリュームはノードの `--backup-interval`（既定 5m、`0` で定期バックアップなし）に従います。

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: hermit-1m
provisioner: hermit.csi.tsuzu.dev
volumeBindingMode: WaitForFirstConsumer
parameters:
  backupInterval: 1m
```

- 値は CreateVolume 時に PV の volume context に記録され、NodeStage のたびにノードへ渡されます。PV は不変なので、StorageClass を後から変えても既存のボリュームには反映されません
- ノードは 10 秒ごとに起き、前回の定期バックアップ開始から間隔が経ったボリュームだけを順番にバックアップします。失敗したら次の間隔で再試行します
- NodeStage の直後は S3 と一致しているので、最初の定期バックアップはそこから 1 間隔後です。ノードプラグインが再起動したときは、最初の周回ですべてのボリュームをバックアップします
- バックアップは 1 つずつ順番に行うので、ボリュームが多く差分が大きいと、実際の間隔は指定より伸びることがあります

## スケジューラプラグイン（ローカルコピーのあるノードを優先）

別ノードで NodeStage されると S3 からの復元が終わるまで Pod が起動しないため、前回そのボリュームを使っていたノードに戻せると速く起動できます。
`scheduler/` は kube-scheduler にスコアプラグイン `HermitLocality` を組み込んだ 2 つ目のスケジューラです。

- コントローラは ControllerUnpublish のときに、外したノードを `owner.json` の `lastNode` に記録します（追加の S3 書き込みはありません）
- プラグインは PreScore で Pod のバインド済み hermit PVC ごとに `owner.json` を読み、アタッチ中ならそのノード、デタッチ後なら `lastNode` を「ローカルコピーのあるノード」とみなします
- Score は、Pod の hermit ボリュームの容量のうち、そのノードにコピーがある割合 × 100 です。フィルタはしないので、そのノードに置けなければ普通に別ノードへ行きます（その場合は復元）
- 未バインド（WaitForFirstConsumer の新規ボリューム）、他ドライバのボリューム、所有情報が無い・読めない場合は何も優先しません。S3 の読み取りは 1 サイクルあたり既定 2 秒で打ち切ります

```sh
docker build -f scheduler/Dockerfile -t ghcr.io/tsuzu/hermit-scheduler:latest .
kubectl apply -k deploy/scheduler/     # deploy/ の ConfigMap / Secret（S3 設定と認証情報）を共有します
```

使う Pod は `spec.schedulerName: hermit-scheduler` を指定します（StatefulSet なら Pod テンプレートに）。
既定のスケジューラはそのまま動き続けます。プラグインの重み（既定 10）は `deploy/scheduler/config.yaml` で調整できます。

## ディレクトリ

```
cmd/hermit-csi/     エントリポイント（--mode=controller|node）
pkg/store/         S3 ラッパー（条件付き書き込み）とテスト用インメモリ実装
pkg/manifest/      S3 上のレイアウトとマニフェスト
pkg/backup/        差分バックアップ／並列復元／GC（LVM 非依存）
pkg/lvm/           lvcreate / thin_delta / thin_dump のラッパーと XML パーサ
pkg/driver/        CSI Identity / Controller / Node、バックアップループ、スナップショットエージェント
deploy/            CSIDriver, RBAC, Controller Deployment, Node DaemonSet, StorageClass
hack/it-setup.sh   結合テスト用に loop デバイス上へ thin pool を作るスクリプト
scheduler/         HermitLocality スケジューラプラグイン（別 Go モジュール。k8s.io/kubernetes に依存）
deploy/scheduler/  hermit-scheduler の設定、RBAC、Deployment
```

## テスト

```sh
go test -race ./...                      # 単体 + E2E（フェイク LVM とインメモリ S3）
```

E2E テストは CSI の RPC を実際の順序で呼び、次の内容を検証します。
- ノード間移動でデータが一致すること
- 小さな書き込みでは 1 チャンクしか上がらないこと
- 復元後のバックアップが増分になること
- 強制デタッチ後のゾンビノードがフェンスされること
- 使用中スナップショットと、スナップショットからの復元
- GC がスナップショット参照を守ること

結合テスト（root、実 LVM、MinIO）:

上の E2E シナリオを実 LVM で動かすほか、Filesystem モード（ext4 / xfs）を検証します。
NodeStage でのフォーマットとマウント、復元後に再フォーマットせずマウントすること、マウント中バックアップの `fsfreeze`（fsync していない書き込みも含まれること）、
フェンス後の旧ノードのアンマウント、使用中スナップショットからの作成、が対象です。`mkfs.ext4`、`mkfs.xfs`、`fsfreeze` が必要です。

```sh
# OSS 版 MinIO はアーカイブ済みで、イメージもバイナリも配布されていないため、最終リリースをソースからビルドする
git clone --depth 1 --branch RELEASE.2025-10-15T17-29-55Z https://github.com/minio/minio.git /tmp/minio-src
(cd /tmp/minio-src && CGO_ENABLED=0 go build -o /tmp/minio .)
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin /tmp/minio server /tmp/minio-data --address 127.0.0.1:9000 &
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://hermit-it
sudo hack/it-setup.sh up
export HERMIT_IT_VG_A=it-a HERMIT_IT_VG_B=it-b HERMIT_IT_S3_ENDPOINT=http://127.0.0.1:9000 \
       HERMIT_IT_BUCKET=hermit-it AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
go test -c -tags integration -o driver.test ./pkg/driver/ && sudo -E ./driver.test -test.run Integration -test.v
sudo hack/it-setup.sh down
```

CI（`.github/workflows/ci.yaml`）は、push と PR のたびに次のジョブを実行します。
- **unit**: gofmt / vet / `-race` テスト / カバレッジ
- **integration**: loop デバイス上の 2 つの thin pool と MinIO を使った結合テスト
- **scheduler**: スケジューラプラグインの vet / テスト（実際の kube-scheduler を fake API サーバーで動かし、Pod の配置先を検証）
- **e2e**: kind クラスタ（ワーカー 2 台、loop デバイス上の thin pool）に deploy/ と deploy/scheduler/ を入れ、実際の kubelet・CSI サイドカー・snapshot-controller・hermit-scheduler で動かす（`test/e2e/run.sh`）。
  Filesystem / Block ボリュームのノード間移動と復元、別ノードの方がリソース的に有利な状況でもローカルコピーのあるノードに戻ること、使用中スナップショットからの復元、削除を確認します
- **image**: コンテナイメージ（ドライバとスケジューラ）のビルド

## デプロイ

1. 対象ノードに VG `hermit` と thin pool `pool` を作り、ラベル `hermit.csi.tsuzu.dev/enabled=true` を付けます。
   ```sh
   vgcreate hermit /dev/nvme1n1
   lvcreate -l 95%FREE --chunksize 64k -T hermit/pool
   ```
2. スナップショットを使う場合は、snapshot CRD と snapshot-controller を入れておきます。
3. `deploy/secret.yaml`（S3 設定と認証情報の ConfigMap / Secret）を編集して `kubectl apply -f deploy/secret.yaml` を、続けて `kubectl apply -k deploy/` を実行します。
4. 動作例は `examples/block-pvc.yaml` にあります。

## 既知の制約 / TODO

- Controller は 1 レプリカ前提です（ロックがプロセス内のため）
- ボリューム拡張（ControllerExpandVolume）は未対応です
- 復元は全量が終わるまで Pod が `ContainerCreating` のままです。ublk / NBD による遅延復元は将来の拡張です
- コントローラとノードエージェント間の通信は Bearer トークン認証の HTTP です。本番では NetworkPolicy か mTLS を推奨します
- コンテナ内では dmeventd による監視を無効にしているため、thin pool の自動拡張（`thin_pool_autoextend_threshold`）は効きません。pool の使用率はノード側で監視してください
- チャンクは非圧縮です（zstd 圧縮は追加しやすい構造になっています）
