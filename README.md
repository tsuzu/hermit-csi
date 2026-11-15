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
| 定期バックアップ | thin スナップショット → `thin_delta` で前回基準との差分ブロックを取得 → 該当チャンクだけ読み、S3 に無いハッシュだけ PUT → `latest.json` を If-Match で差し替え |
| NodeUnstage | アンマウント後に最終バックアップ。LV は残す（同じノードに戻れば復元不要） |
| ControllerUnpublish | 所有を解除してエポック +1（ノード障害時の強制デタッチでも旧ノードを締め出す） |
| CreateSnapshot | デタッチ中なら `latest.json` のコピー。使用中なら所有ノードのエージェントに依頼し、その時点の差分を上げてからマニフェストを固定 |
| GC（controller） | どのマニフェストからも参照されず、猶予期間（既定 24h）を過ぎたチャンクを削除 |

全量アップロードは起きません。初回は割り当て済みブロック（`thin_dump`）だけを読み、ゼロのチャンクは送りません。
基準スナップショットを失った場合も、ローカルを読み直してハッシュが変わった分だけを上げます。

整合性について:
- Filesystem モードはスナップショット前に `fsfreeze` するので、ファイルシステムとして整合した状態になります
- Block モードはクラッシュ整合（電源断と同等）です
- **ノードが突然死んだ場合**は、最後のバックアップ以降の書き込みが失われます（RPO = `--backup-interval`）

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
- **image**: コンテナイメージのビルド

## デプロイ

1. 対象ノードに VG `hermit` と thin pool `pool` を作り、ラベル `hermit.csi.tsuzu.dev/enabled=true` を付けます。
   ```sh
   vgcreate hermit /dev/nvme1n1
   lvcreate -l 95%FREE --chunksize 64k -T hermit/pool
   ```
2. スナップショットを使う場合は、snapshot CRD と snapshot-controller を入れておきます。
3. `deploy/secret.yaml` を編集して `kubectl apply -k deploy/` を実行します。
4. 動作例は `examples/block-pvc.yaml` にあります。

## 既知の制約 / TODO

- Controller は 1 レプリカ前提です（ロックがプロセス内のため）
- ボリューム拡張（ControllerExpandVolume）は未対応です
- 復元は全量が終わるまで Pod が `ContainerCreating` のままです。ublk / NBD による遅延復元は将来の拡張です
- コントローラとノードエージェント間の通信は Bearer トークン認証の HTTP です。本番では NetworkPolicy か mTLS を推奨します
- チャンクは非圧縮です（zstd 圧縮は追加しやすい構造になっています）
