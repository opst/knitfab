# Knitfab 管理ガイド: 1.インストール編 <!-- omit in toc -->

もくじ
- [1. はじめに](#1-はじめに)
  - [1.1. 他言語版/Translations](#11-他言語版translations)
  - [1.2. 重要な注意点](#12-重要な注意点)
- [2. Knitfab インストールの事前準備](#2-knitfab-インストールの事前準備)
  - [2.1. Kubernetesのインストール](#21-kubernetesのインストール)
  - [2.2. CNI をインストールする](#22-cni-をインストールする)
  - [2.3. GPU を有効化する](#23-gpu-を有効化する)
  - [2.4. シングルノードクラスタでの設定](#24-シングルノードクラスタでの設定)
  - [2.5. NFS サーバの用意](#25-nfs-サーバの用意)
  - [2.6. その他ツール類](#26-その他ツール類)
- [3. Knitfab をインストールする](#3-knitfab-をインストールする)
  - [3.1. インストールされるもの](#31-インストールされるもの)
  - [3.2. 必要なもの](#32-必要なもの)
  - [3.3. インストール手順](#33-インストール手順)
- [4. Knitfab をアンインストールする](#4-knitfab-をアンインストールする)
- [5. Knitfab の helm 的構成について](#5-knitfab-の-helm-的構成について)


# 1. はじめに

この文書は Knitfab を運用・管理する人に向けたものです。

- Knitfab をインストールする方法
- Knitfab の運用上の注意点
- Knitfab を構成する Kubernetes リソースについて

などの話題を取り扱います。ファイルは第１部と第２部に分かれています。第１部は主に
インストール手順について説明し、第２部は主にインストール後のKnitfabの運用管理に
関する内容を説明しています。

## 1.1. 他言語版/Translations
他言語版は以下のリンク先にあります。

- English:
  - [./admin-guide-installation.en.md](./admin-guide-installation.en.md)
  - [./admin-guide-deep-dive.en.md](./admin-guide-deep-deive.en.md)

## 1.2. 重要な注意点

> [!Caution]
>
> **Knitfab をパブリックなネットワークに公開してはいけません。**
>
> 現時点の Knitfab やクラスタ内イメージレジストリには、認証や認可の仕組みが一切ありません。
>
> パブリックなインターネットに公開すると、次のリスクがあります。
>
> - 悪意あるコンテナを実行される
> - 悪意あるコンテナイメージを配信される
>
> 前者は、計算機資源を奪われるだけでなく、Kubernetes の未知の脆弱性を突かれてさらなる脅威にさらされる可能性があります。
> 後者も、他の脅威の踏み台になりかねません。
>
> **重ねて警告します。Knitfab をパブリックなインターネットに公開してはいけません。**
>

# 2. Knitfab インストールの事前準備

Knitfab をインストールするには、以下の環境が必要です。

- Kubernetes クラスタ: 
  - **Kubernetes**（クバネティス/クバネテス/クーべネティス、K8sと略記されます）は、コンテナ化したアプリケーションのデプロイ、スケーリング、および管理を行うための、オープンソースのコンテナオーケストレーションシステムです。
  - Knitfab は Kubernetes クラスタ上で稼働します。
  - マルチノードクラスタまたはシングルノードクラスタでも構いません。
  - この Kubernetes は、x86_64 系CPUで動作するマシン上で動作している必要があります。
  - クラスタからインターネットにアクセスできる必要があります。
- NFS:
  - **NFS(Network File System)** は主にUNIXで利用される分散ファイルシステムおよびそのプロトコルです。
  - Knitfabが用いる RDB やクラスタ内イメージレジストリ、データなどを永続化するために、NFS を利用します。

特に NFS は、Knitfabがデータ履歴等を蓄積していく場所となりますので、十分な容量があるものが良いでしょう。

## 2.1. Kubernetesのインストール

Kubernetes の構築手法については、下記の公式リファレンスを参考にしてください。

- https://Kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/
- https://Kubernetes.io/docs/setup/production-environment/container-runtimes/
- https://Kubernetes.io/docs/tasks/administer-cluster/kubeadm/configure-cgroup-driver/

なお、Knitfab 開発チームでは、次の条件で構築した Kubernetes クラスタについて動作を確認しています。

- Kubernetes 1.29.2以降
- コンテナランタイム: containerd
- cgroup: systemd

## 2.2. CNI をインストールする

kubenetes 上のネットワーク機能を有効化するために、何らかの CNI (container network interface) をインストールする必要があります。

Knitfab 開発チームは [calico](https://docs.tigera.io/calico/latest/about) で動作を確認しています。

## 2.3. GPU を有効化する

Kubernetes 上のコンテナから GPU を使えるようにするには、node をそのように設定しておく必要があります。

これも下記の公式リファレンスを参考にしながら設定を行ってください。

- https://Kubernetes.io/ja/docs/tasks/manage-gpus/scheduling-gpus/

## 2.4. シングルノードクラスタでの設定

Kubernetes クラスタを単一ノード (control plane ノード) のみのクラスタで運用し始める場合は、そのノードに指定されている taint を除去する必要があります。
これを行わないと Knitfab のコンポーネントが起動できるノードが存在しない、という状態になります。

詳細は https://Kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/#control-plane-node-isolation を参照ください。

## 2.5. NFS サーバの用意

Knitfab では、デフォルトの [ストレージクラス](https://Kubernetes.io/docs/concepts/storage/storage-classes/) として、ストレージドライバ [csi-driver-nfs](https://github.com/Kubernetes-csi/csi-driver-nfs) によるものを採用しています。これはコンテナがどの Kubernetes ノードで起動したとしても Knitfab がデータにアクセスできるようにするためです。

NFSにはいくつかバージョンがありますが、Knitfab では NFS version 4 を前提としています。

そこで、Kubernetes クラスタの各ノードからアクセス可能なネットワーク上の位置に、NFSサーバ を用意してください。
例えば、NFS 機能を有する NAS(Network Attached Storage)機器や、NFSサーバー機能を有する計算機（Linuxサーバなど）等です。

> たとえば Ubuntu OS マシンをNFSサーバーとするなら:
>
> - `nfs-kernel-server` パッケージをインストールして (`apt install nfs-kernel-server`) 、
> - `/etc/exports` に設定ファイルを配置することで
>
> NFS サーバにできます。

## 2.6. その他ツール類
以下のツールをインストールしてください。

- [helm](https://helm.sh/)
- bash
- wget
- jq

インストール方法は、お使いのOSのドキュメントや関連資料を参照ください。


# 3. Knitfab をインストールする

## 3.1. インストールされるもの

インストール手順を実施することにより、次のものが Kubernetes クラスタにインストールされます。

|  | 対応する Helm Chart |
|:------|:------------|
| Knitfab アプリケーション本体 | knit-app, knit-schema-upgrader |
| データベース | knit-db-postgres |
| クラスタ内イメージレジストリ | knit-image-registry |
| TLS 証明書類 | knit-certs |
| ストレージクラス | knit-storage-nfs |

また、 Helm Chart "knit-storage-nfs" は [CSI "csi-driver-nfs"](https://github.com/Kubernetes-csi/csi-driver-nfs/) に依存しているので、この Chart も Knitfab とおなじ Namespace にインストールされます。

## 3.2. 必要なもの
- インストール先の Kubernetes クラスタに対してアクセスできる設定の kubeconfig ファイル
- (単一ノードクラスタを構成の場合)そのノードのマシンに 4GB のメモリが必要。
  なお、このメモリ量は最低限 Knitfab が起動できる程度の容量です。Knitfab上で実行する機械学習タスクによっては、より多くのメモリが必要となります。

### 3.2.1. (任意実施) TLS 証明書を用意する

Knitfab Web API やクラスタ内イメージレジストリは、原則として https で通信を行います。
デフォルトではインストールスクリプトはそのための証明書を生成しますが、必要に応じて別の証明書を指定して使うこともできます。

- CA 証明書とその鍵があれば、それを使う
- 加えて、サーバ証明書とその鍵があれば、それを使う

たとえば「 Kubernetes クラスタのノードに対して特定のドメイン名が使いたい」などといった要求があるなら、事前にサーバ証明書とそれに署名した CA 証明書 (およびそれらの鍵) が必要です。

証明書類が指定されない場合は、インストーラは自己署名証明書と、それで署名したサーバ証明書を生成します。サーバ証明書は、Knitfab をインストールした際の Kubernetes クラスタのノードの IP アドレスを SAN に持つように生成します。

## 3.3. インストール手順
以下の順序で実施します。

1. インストーラを手に入れます。
2. インストール設定ファイルを生成し、パラメータを調整します。
3. インストールを実行します。
4. ユーザにハンドアウトを配布し、利用開始してもらいます。

### 3.3.1. インストーラを手に入れる

インストーラは https://github.com/opst/knitfab/installer/installer.sh です。

これを適当なディレクトリ ( 本書では、例として`~/knitfab` としますが、別の場所でも構いません) にダウンロードします。

```
mkdir -p ~/knitfab/install
cd ~/knitfab/install
wget -O installer.sh https://raw.githubusercontent.com/opst/knitfab/main/installer/installer.sh
chmod +x ./installer.sh
```

### 3.3.2. インストール設定ファイルを生成し、パラメータを調整する
以下を実行すると、 `./knitfab-install-settings` ディレクトリに Knitfab のインストール設定が生成されます。
`${YOUR_KUBECONFIG}` の部分は、事前に用意した kubeconfig ファイルへのパスを指定してください。

```
./installer.sh --prepare --kubeconfig ${YOUR_KUBECONFIG}
```


> [!Note]
>
> もし特定の TLS 証明書類を利用したいなら、代わりに次のコマンドを実行してください。
>
> ```
> TLSCACERT=path/to/ca.crt TLSCAKEY=path/to/ca.key TLSCERT=path/to/server.crt TLSKEY=path/to/server.key ./installler.sh --prepare --kubeconfig ${KUBECONFIG}
> ```
>
> サーバ証明書について指定がないなら、環境変数 `TLSCERT`, `TLSKEY` を省略して、次のようにします。
>
> ```
> TLSCACERT=path/to/ca.crt TLSCAKEY=path/to/ca.key ./installler.sh --prepare
> ```
>
> CA 証明書が指定されなかった場合には、インストーラは 自己署名証明書を自動的に生成します。
> サーバ証明書が指定されなかった場合には、インストーラは CA 証明書から自動的に生成します。

> [!Note]
>
> **上級向け**
>
> 上記の手順は、 Knitfab Web API を https として公開するように設定するものです。
>
> 一方で、Knitfab 自身が https 化されていると不都合な場合もあります。たとえ
> ば、Knitfab Web API の前にロードバランサーを設置して、TLS終端化はそちらで行い
> たい、という場合です。
>
> そうした場合には、次のように、フラグ `--no-tls` を付加して「手順2」を実行してください。
>
> ```
> ./installer.sh --prepare --no-tls --kubeconfig ${YOUR_KUBECONFIG}
> ```
>
> これによって、 `./installer.sh --prepare` が TLS 証明書ならびに関連する設定を生成しないようになり、続くインストール時にも Knitfab Web API は https 化されません。
>
> なお、この際にはクラスタ内イメージレジストリも https 化されないので、各ユーザはインセキュアレジストリ(insecure registry)として dockerd に登録する必要があります。その詳細は、次のリンク先を参照ください。
>
> - https://docs.docker.com/reference/cli/dockerd/#insecure-registries
> - https://docs.docker.com/reference/cli/dockerd/#daemon-configuration-file
>

> [!Caution]
>
> **TLS証明書を指定した場合、それら証明書や秘密鍵がインストール設定の一部として以下の場所に複製されます。**
>
> - `knitfab-install-settings/certs/*` (キーペア; ファイルのコピーとして)
> - `knitfab-install-settings/values/knit-certs.yaml` (キーペア; base64エンコードされたテキストとして)
> - `knitfab-install-settings/knitprofile` (証明書のみ; base64エンコードされたテキストとして)
>
> また、インストーラが証明書を自動生成した場合も上記の場所に配置されます。
>
> 特に、キーペアには **秘密鍵** が含まれるので、取り扱いには注意してください。

#### 3.3.2.1. NFS を設定する

**インストーラで生成されるデフォルト設定は、「 Knitfab が管理している情報を永続化しない」ようになっています。**

つまりデフォルトでは NFS を使用しない設定です。
そこで、Knitfab用に用意した NFS を利用してデータを永続化するように、設定を変更します。

変更すべきファイルは `knitfab-install-settings/values/knit-storage-nfs.yaml` です。
このファイル内の次のエントリを変更してください。

- `nfs.external`: 値を `true` にする。
- `nfs.server`: をコメント解除して、 NFS サーバのホスト名 (ないし IPアドレス) を指定する。

さらに、必要に応じて次のエントリも変更してください。

- `nfs.mountOptions`: NFS に対するマウントオプションについて特に指定があれば記述します。
- `nfs.share`: Knitfab に利用させたいサブディレクトリがあれば指定します。
    - 注：そのサブディレクトリは、事前に作成しておく必要があります。

以上をまとめると、`knit-storage-nfs.yaml` は、次のようになるでしょう。

```yaml
nfs:
  # # external: If true (External mode), use NFS server you own.
  # #  Otherwise(In-cluster mode), knitfab employs in-cluster NFS server.
  external: true

  # # mountOptions: (optional) Mount options for the nfs server.
  # #  By default, "nfsvers=4.1,rsize=8192,wsize=8192,hard,nolock".
  mountOptions: "nfsvers=4.1,rsize=8192,wsize=8192,hard,nolock"

  # # # FOR EXTERNAL MODE # # #

  # # server: Hostname of the nfs server.
  # #  If external is true, this value is required.
  server: "nfs.example.com"  # update this to your NFS server host.

  # # share: (optional) Export root of the nfs server. default is "/".
  share: "/"

  # # # FOR IN-CLUSTER MODE # # #

  # # hostPath: (optional) Effective only when external is false.
  # # If set, the in-cluster NFS server will read/write files at this directory ON NODE.
  # #
  # # This is useful when you want to keep the data even after the NFS server is restarted.
  # hostPath: "/var/lib/knitfab"

  # # node: (optional) Kubernetes node name where the in-cluster NFS server pod should be scheduled.
  # node: "nfs-server"
```

#### 3.3.2.2. その他のインストール時の設定

ここまで述べた以外のファイルについても、必要に応じてパラメータを変更できます。

特に利用上影響があるのは次のものです。
##### (1) ポート番号

- `knitfab-install-settings/values/knit-app.yaml` の `knitd.port`
- `knitfab-install-settings/values/knit-image-registry.yaml` の `port`

前者は Knitfab API の LISTEN ポート、後者はクラスタ内イメージレジストリの LISTEN ポートです。

##### (2) クラスタのTLD
また、 Kubernetes クラスタ構築時に、クラスタの TLD(Top Level Domain)をデフォルト
値 ( `cluster.local` ) から変更していた場合には、次の項目にその TLD を設定する必
要があります。

- `knitfab-install-settings/values/knit-app.yaml` の `clusterTLD` (コメント解除して書き換えます)

##### (3) Knitfab 拡張機能関連
Knitfab の動作を拡張するための設定ファイルも含まれています。

- `knitfab-install-settings/values/hooks.yaml` を編集することで WebHook を設定できます。
- `knitfab-install-settings/values/extra-api.yaml` を編集することで拡張 Web API を設定できます。

詳細は、「Knitfab を拡張する」の章を参照ください。

### 3.3.3. インストールする

以下のコマンドを実行することで、インストールスクリプトが順次 Knitfab のコンポー
ネントを Kubernetes クラスタにインストールします。 `${NAMESPACE}`には、Knitfabア
プリケーションのインストール先とする Kubernetes 名前空間名を指定してくださ
い。（ここで新規に指定します。）これには、**しばらく時間がかかります。**

```
./installer.sh --install --kubeconfig path/to/kubeconfig -n ${NAMESPACE} -s ./knitfab-install-settings
```

インストールが成功したかどうかは、K8s deployment の状態から分かります。

以下のようにして、`kubectl get deploy -A` コマンドでKnitfabをインストールした
ネームスペース内の deployment の'READY'値を確認し、それらがすべて'N/N'のように分
母と分子の値が一致していれば成功です。

```
$ kubectl get deploy -A
NAMESPACE          NAME                        READY   UP-TO-DATE   AVAILABLE   AGE
calico-apiserver   calico-apiserver            2/2     2            2           21d
calico-system      calico-kube-controllers     1/1     1            1           21d
calico-system      calico-typha                2/2     2            2           21d
kf-mycluster       csi-nfs-controller          1/1     1            1           19d
kf-mycluster       database-postgres           1/1     1            1           19d
kf-mycluster       finishing-leader            1/1     1            1           19d
kf-mycluster       garbage-collection-leader   1/1     1            1           19d
kf-mycluster       housekeeping-leader         1/1     1            1           19d
kf-mycluster       image-registry-registry     1/1     1            1           19d
kf-mycluster       initialize-leader           1/1     1            1           19d
kf-mycluster       knitd                       1/1     1            1           19d
kf-mycluster       knitd-backend               1/1     1            1           19d
kf-mycluster       projection-leader           1/1     1            1           19d
kf-mycluster       run-management-leader       1/1     1            1           19d
kube-system        coredns                     2/2     2            2           21d
tigera-operator    tigera-operator             1/1     1            1           21d

```
上記の例では、`kf-mycluster` というネームスペースで Knitfab をインストールしてい
ます。このネームスペースに属する deployment(NAME列)のREADY値がすべて '1/1' と
なっているのが確認できます。

この状態になるまでしばらく時間がかかる場合がありますので、READY値が'N/N'になって
いない場合は少し時間置いて再度試してみてください。

それでも駄目な場合は、[トラブルシュート](admin-guide-deep-dive.ja.md#5-トラブルシュート)
などを参考にして対処してください。

Knitfabと関係のないネームスペースの情報を表示させたくない場合は、以下のように
`-n` オプションを使用してください。
```
$ kubectl get deploy -n kf-mycluster
```
'kf-mycluster' の部分は、あなたのシステムのネームスペース名で書き換え
てください。


### 3.3.4. ユーザにハンドアウト(Knitfab設定情報)を配布する

インストールされた Knitfab への接続情報が `knitfab-install-settings/handouts` フォルダに生成されます。

このフォルダの内容を、Knitfab を使用するユーザに配布してください。これを**ハンドアウト**と呼びます。

このハンドアウトの使い方については、 user-guide に説明があります。

#### (任意実施)3.3.4.1. ハンドアウトを修正する

Knitfab に対して特定のドメイン名でアクセスしたい場合には (例: 指定したサーバ証明
書がそうなっている場合) 、ユーザにハンドアウトを配布する前に、接続設定を書き換え
る必要があります。

**knitprofile ファイル** と呼ばれる、Knitfab API への接続設定が
`knitfab-install-settings/handouts/knitprofile` にあります。このファイルは次のよ
うな構成をした yaml ファイルです。

```yaml
apiRoot: https://IP-ADDRESS:PORT/api
cert:
    ca: ...Certification....
```

キー `apiRoot` の値が、Knitfab Web API のエンドポイントを示します。
デフォルトでは、クラスタの適当なノードの IP がセットされています。
特定のドメイン名でアクセスしたいなら、ここにそれを記入してください。

たとえば Knitfab に対して `example.com:30803` としてアクセスしたいなら、

```yaml
apiRoot: https://example.com:30803/api
cert:
    ca: ...Certification....
```

のように、 `apiRoot` のホスト部分を書き換えます。

また、**クラスタ内イメージレジストリ** の証明書についても対処が必要です。

`knitfab-install-settings/handouts/docker/certs.d/IP-ADDRESS:PORT` のような名前のディレクトリがあります。
このディレクトリ名は Kubernetes ノードの IPアドレスとポート名を `:` でつないだものです。
この IPアドレスの部分を、使用したいドメイン名に変更してください。

# 4. Knitfab をアンインストールする

インストールを実行すると `knitfab-install-settings/uninstall.sh` としてアンインストーラが生成されます。

これを以下のように実行すると、Kubernetes クラスタ内の Knitfab のアプリケーションがアンインストールされます。

```
knitfab-install-settings/uninstall.sh
```

さらに、以下を実行すると、データベースやクラスタ内イメージレジストリを含むすべての Knitfab 関連リソースが破棄されます。

```
knitfab-install-settings/uninstall.sh --hard
```


# 5. Knitfab の helm 的構成について

Knitfab はいくつかの helm chart で構成されています。
このセクションでは、Knitfab の helm 的な構築方法について解説します。

管理者は Knitfab の一部をアンインストール・再インストールしたり、アップデートし
たりしなくてはならない場合があるかもしれません。helm構成を理解しておけば、そうし
た場合に何をすればよいか見通しが立つようになるでしょう。

> [!Note]
>
> このセクションは、読者に helm の知識があることを前提としています。

Knitfab は次の helm chart から構成されています。

- knitfab/knit-storage-nfs: NFS ドライバを導入し StorageClass を定義する。
- knitfab/knit-certs: 証明書類を導入する。
- knitfab/knit-db-postgres: RDB を定義する。
- knitfab/knit-image-registry: クラスタ内レジストリを定義する。
- knitfab/knit-app: 上記以外の Knitfab のコンポーネントを定義する。

helm chart リポジトリ "Knitfab" は (デフォルトでは)  https://raw.githubusercontent.com/opst/knitfab/main/charts/release です。

これらの chart を適切な手順でインストールすれば、Knitfab をインストールできます。
実際、Knitfabの インストーラはまさにそれを実行しています。

要点だけに絞ると、インストーラは以下ような手順を実施しています。

```sh
NAMESPACE=${NAMESPACE}  # where Knitfab to be installed
CHART_VERSION=${CHART_VERSION:=v1.0.0}  # version of Knitfab to be installed
VALUES=./knit-install-settings/values

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    -f ${VALUES}/knit-storage-nfs.yaml \
    knit-storage-nfs knitfab/knit-storage-nfs

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    -f ${VALUES}/knit-certs.yaml \
    knit-certs knitfab/knit-certs

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    --set-json "storage=$(helm get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
    -f ${VALUES}/knit-db-postgres.yaml \
    knit-db-postgres Knitfab/knit-db-postgres

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    --set-json "storage=$(helm get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
    --set-json "certs=$(helm get values knit-certs -n ${NAMESPACE} -o json --all)" \
    -f ${VALUES}/knit-image-registry.yaml \
    knit-image-registry Knitfab/knit-image-registry

helm install -n ${NAMESPACE} --version ${CHART_VERSION} \
    --set-json "storage=$(helm get values knit-storage-nfs -n ${NAMESPACE} -o json --all)" \
    --set-json "database=$(helm get values knit-db-postgres -n ${NAMESPACE} -o json --all)" \
    --set-json "imageRegistry=$(helm get values knit-image-registry -n ${NAMESPACE} -o json --all)" \
    --set-json "certs=$(helm get values knit-certs -n ${NAMESPACE} -o json --all)" \
    -f ${VALUES}/knit-app.yaml \
    knit-app Knitfab/knit-app
```

> 実際のインストーラは以上の操作に加えて、これらの挙動をもっと安定させるために追
> 加のオプションを与えたり、アンインストーラやハンドアウトを生成したりしていま
> す。

上記の途中にたびたび現れている `--set-json "...=$(helm get values ...)"` というパターンは、インストール済の chart からインストールパラメータ ([helm の Values](https://helm.sh/docs/chart_template_guide/values_files/)) を読み出して、chart 間で矛盾がないようにする手法です。

それに加えて `./knitfab-install-settings/values/CHART_NAME.yaml` をその chart 用
の Values として取り込んでいます。したがって、特定の chart のみを再インストール
したり、アップデートしたりする必要に迫られた場合は、この手法を踏襲するのが良いで
しょう。

> [!Caution]
>
> 次の chart をアンインストールすると、Knitfab 内の**リネージやデータを喪失してしまいます**。chart をアンインストールする際には注意ください。
>
> - knitfab/knit-storage-nfs
> - knitfab/knit-db-postgres
> - knitfab/knit-image-registry
>
> knit-db-postgres や knit-image-registry は、それぞれ PVC も定義しているので、これらの chart をアンインストールすると、それまでのデータベースの内容や、`docker push` されたイメージが**失われます**。
> 結果として、PVC と Knitfab 的なデータとの関係や、プランが参照するイメージが失われるので、Knitfab のリネージ管理の前提が満たされないことになります。
>
> また、knit-storage-nfs は他の全ての PV を NFS 上に記録する機能を提供しています。これが失われると、全 Pod が PV にアクセスできなくなります。