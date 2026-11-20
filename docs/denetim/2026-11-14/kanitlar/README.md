# Kanıt ve tekrar çalıştırma rehberi

Bu paket `d1259449bb1ad20d2df2626989b3a581b7e7893f` commit'inin 14 Kasım 2026 denetimine aittir. Deneyler uygulama kaynaklarını değiştirmeden yazılmıştır. Deney betikleri gerçek kullanıcı verisine karşı çalıştırılmamalıdır; HTTP betiği kendi geçici redb dosyasını oluşturur. CouchDB betiği yalnız kendi rastgele adlı veritabanını siler; yine de ayrı test sunucusu tercih edilmelidir.

## Dosyalar

| Dosya | İçerik |
|---|---|
| `validation-summary.json` | Test toplamları, komut çıkış kodları, çalışma ortamı |
| `source-manifest.json` | İncelenen commit'teki crate/test/CI dosyalarının SHA-256 envanteri; manuel okuma veya coverage listesi değildir |
| `tests.log` | Varsayılan workspace testleri, doctest dahil |
| `arbitrary-precision.log` | Aynı suite'in feature açık koşusu |
| `couchdb.log` | Gerçek CouchDB gerektiren 130 test |
| `couchdb-numbers.log` | Feature açık büyük sayı round-trip referans testi |
| `fmt.log`, `clippy.log`, `build.log` | Format, lint ve HTTP server build sonuçları |
| `pouchdb-runner.log`, `pouchdb/t*.json` | PouchDB T1–T9 sonuçları ve senaryo süreleri |
| `pouchdb/t*-server.log`, `*.stderr`, `*.exit`, `*.pid` | Runner'ın bıraktığı tanılama dosyaları; pid dosyaları tarihsel, çalışan süreç belirtmez |
| `http-probes.py`, `http-probes.json` | İzole HTTP deneyleri, yanıtlar ve beklenen kusurlara ait panic logları |
| `couchdb-reference-probes.py`, `couchdb-reference-probes.json` | Gerçek CouchDB karşılaştırması: tombstone, büyük sayı, nil bookmark |
| `library-probes/` | Bağımsız Rust yeniden üretim programı ve lockfile |
| `library-probes.jsonl` | Normal mod: ortak-handle reset, zero batch, boş indeks |
| `library-probes-arbitrary-precision.jsonl` | Yukarıdakiler + büyük sayının yanlış eşitlik sonucu |
| `gitnexus-refresh.log` | İndeks yenilemesi; flow truncation ve çözümleme sınırı uyarıları da korunmuştur |
| `rouchdb-graph-*.json` | Kavram sorguları, sembol context ve upstream impact çıktıları; ambiguous/UNKNOWN sonuçlar da bulunur |

Geçici port, UUID, revision hash ve zaman alanları her çalışmada değişebilir. HTTP panic kayıtları özellikle korunmuştur; bu deneylerin sorunu gösterebilmesi için debug derleme kullanılmıştır. Tüm sunucunun çöktüğü şeklinde yorumlanmamalıdır.

## Online kaynak doğrulamasının ek kanıtları

[Dokuz maddenin değerlendirmesi](../ONLINE-KAYNAK-DOGRULAMASI.md) online kaynaklar okunduktan sonra güncellendi. Eski deney çıktıları gözlem kaydı olarak korundu; tüm gözlemler hata olarak sınıflandırılmıyor. Özellikle F07 CouchDB ile uyumlu, F09’daki Rust kabulü upstream ile uyumludur.

| Dosya | İçerik |
|---|---|
| `online-source-manifest.json` | CouchDB 3.5.1/PouchDB 9.0.0 commit, URL, SHA-256 ve incelenen satır aralıkları |
| `upstream-reference-probes.py`, `.json` | Ek gerçek CouchDB deneyleri: indeks kimliği/boş fields, forwarded Location, büyük revision, purge state |
| `pouchdb-upstream-probes.cjs`, `.json` | PouchDB 9 local: boş fields, indeks kimliği/no-op, zero batch, handle destroy, tombstone ve JS sayıları |

Ek CouchDB betiği önceki referans betiği gibi `COUCHDB_URL` alır ve yalnız oluşturduğu rastgele isimli DB’yi siler. PouchDB betiği RAM kullanır. `POUCHDB_FIND_DIST`, manifestteki commit’e sabit resmî `dist/pouchdb.find.js` dosyasının indirilmiş yerel yolunu göstermelidir; küçültülmemiş browser dağıtımı global alias ile Node içinde yüklenir. Örnek:

```bash
curl -fsSL 'https://raw.githubusercontent.com/pouchdb/pouchdb/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.find.js' -o /tmp/pouchdb-find-9.0.0-audit.js
POUCHDB_FIND_DIST=/tmp/pouchdb-find-9.0.0-audit.js \
  node docs/denetim/2026-11-14/kanitlar/pouchdb-upstream-probes.cjs
python3 docs/denetim/2026-11-14/kanitlar/upstream-reference-probes.py
```

Bu online inceleme turunda geniş RouchDB suite’leri yeniden çalıştırılmadı. Aşağıdaki komutlar ilk denetimin yeniden üretimi içindir.

## Mevcut testleri çalıştırma

Komutları repository kökünden çalıştırın:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked --no-fail-fast
cargo test --workspace --locked --no-fail-fast --features rouchdb/arbitrary-precision
```

PouchDB bağımlılıkları hazır değilse `npm ci` gerekir. Ardından:

```bash
cargo build -p rouchdb-server --locked
ROUCHDB_SERVER_BIN="$PWD/target/debug/rouchdb-server" \
  ROUCHDB_TEST_OUTPUT_DIR="/tmp/rouchdb-pouchdb-rerun" \
  bash run-tests.sh
```

T4 en az yaklaşık 90 saniye sürer. T8/T9 da ek süre gerektirir. Yeni çıktıyı orijinal kanıt klasörünün üzerine yazmamak için yukarıdaki ayrı output dizini kullanılmıştır.

## HTTP deneyleri

```bash
cargo build -p rouchdb-server --locked
python3 docs/denetim/2026-11-14/kanitlar/http-probes.py \
  > /tmp/rouchdb-http-probes-rerun.json
```

Betiğin çalışma alanı Python `TemporaryDirectory` içindedir. Binary dinlemek için port 0 kullanır; süreç kendi `finally` bloğunda sonlandırılır. F01/F04/F05/F07/F08 davranışlarını ve birkaç karşı kontrolü kaydeder. Çıkış kodu yalnız betiğin tamamlandığını gösterir; her kaydın ürün doğruluğu açısından başarılı olduğu anlamına gelmez.

## Rust deneyleri

Bağımsız Cargo workspace'in lockfile'ı kök lockfile çözümünden türetilmiştir. Binary çıktısını kök `target/` altında tutmak için:

```bash
CARGO_TARGET_DIR="$PWD/target" cargo run --locked \
  --manifest-path docs/denetim/2026-11-14/kanitlar/library-probes/Cargo.toml

CARGO_TARGET_DIR="$PWD/target" cargo run --locked \
  --manifest-path docs/denetim/2026-11-14/kanitlar/library-probes/Cargo.toml \
  --features arbitrary-precision
```

Bu program kusurları doğrulayan assertion'lar ve API farklarını kaydeden çıktılar içerir; boş indeks kabulü upstream uyumlu olduğundan tek başına kusur değildir. Assertion'lar: reset deneyinde indexed sonuç boş olmalıdır; zero batch deneyinde hedef boşken `ok=true` görülmelidir; büyük sayı deneyinde yanlış olarak iki belge gelmelidir. Kusur düzeltildiğinde ilgili assertion'ın başarısız olması beklenir. Bunlar normal regression suite'ine alınırken doğru davranış beklentisine çevrilmelidir.

## İzole CouchDB referansı

Denetimde Docker'daki CouchDB **3.5.1**, ayrı container ve loopback ephemeral port ile kullanıldı. Aşağıdaki örnek test parolasını üretim parolasıyla değiştirmeyin:

```bash
docker run --detach --rm --name rouchdb-audit-reference \
  --publish 127.0.0.1::5984 \
  --env COUCHDB_USER=admin --env COUCHDB_PASSWORD=audit-local-password \
  couchdb:3.5.1
docker port rouchdb-audit-reference 5984
```

İkinci komuttaki portu `COUCHDB_URL` içine koyun. Aşağıda `<PORT>` yer tutucudur; `_up` hazır olana kadar bekleyin:

```bash
export COUCHDB_URL='http://admin:audit-local-password@127.0.0.1:<PORT>'
curl -fsS "$COUCHDB_URL/_up"
curl -fsS -X PUT "$COUCHDB_URL/_node/_local/_config/chttpd_auth_lockout/mode" -d '"off"'
bash scripts/test-couchdb.sh --locked
cargo test -p rouchdb --test big_numbers --locked --features arbitrary-precision -- --ignored
python3 docs/denetim/2026-11-14/kanitlar/couchdb-reference-probes.py \
  > /tmp/rouchdb-reference-rerun.json
docker stop rouchdb-audit-reference
```

`couchdb-reference-probes.py`, `COUCHDB_URL` üzerinden credentials alır ve çıktılarına bunları yazmaz. `rouchdb_audit_manual_<uuid>` veritabanını yaratır ve `finally` içinde siler. Orijinal denetim container'ı iş sonunda durdurulup kaldırılmıştır.

## Grafiği yeniden kontrol etme

```bash
node .gitnexus/run.cjs status
node .gitnexus/run.cjs query 'replication checkpoint bulk_docs' --repo . --limit 3
node .gitnexus/run.cjs context run_replication --file crates/rouchdb-replication/src/protocol.rs --repo .
node .gitnexus/run.cjs impact run_replication --file crates/rouchdb-replication/src/protocol.rs --direction upstream --repo .
```

İndeks bayatsa proje talimatlarına göre yenileyin. Ambiguous sembol sonucunda UID kullanın; UNKNOWN veya boş caller listesi güvenli değişiklik kanıtı değildir. Denetimde flow çıkarımı truncation uyarısı içerdiğinden grafiğin kapsamadığı davranışların yokluğu iddia edilmemiştir.
