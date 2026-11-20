# RouchDB — kanıta dayalı kaynak kodu denetim raporu

**Tarih:** 14 Kasım 2026, Europe/Istanbul. **Sürüm:** 0.5.1. **Commit:** `d1259449bb1ad20d2df2626989b3a581b7e7893f`.

## 1. Yönetici değerlendirmesi

RouchDB'nin depolama, revision ağacı ve CouchDB replikasyon çekirdeği çalışır durumdadır; yerel denetimde geniş mevcut test seti başarıyla tamamlanmıştır. Kenar durumlarını hedefleyen deneyler ve ardından resmî online kaynaklarla yapılan sorgulama sonunda **7 davranış hatası** korunmuştur: **3 P1, 3 P2, 1 P3**. İlk dokuz maddeden F07 **sertleştirme önerisi**, F09 **API uyum farkı** olarak yeniden sınıflandırılmıştır. Özellikle tombstone gövdesinin kaybı, arbitrary-precision modunda yanlış eşitlik ve paylaşılan depolamanın reset'i sonrası eksik sorgu sonucu üretimi veri doğruluğunu etkiler.

Bu sonuç bir üretim uygunluğu onayı değildir. Varsayılan CRUD/replikasyonun testlerde çalışması, bütün JSON değerlerinde, farklı handle yaşam döngülerinde veya güvenilmeyen input'ta sözleşmelerin korunduğunu göstermemektedir. Üretim önceliği ilk üç doğruluk hatası, ardından indeks tutarlılığı, aritmetik doğrulama ve replikasyon parametre kontrolü olmalıdır.

Dokuz maddenin resmî CouchDB/PouchDB kaynaklarıyla tek tek değerlendirmesi [online kaynak doğrulamasındadır](ONLINE-KAYNAK-DOGRULAMASI.md). Özellikle F07 ve F09’un önceki hata gerekçeleri geri çekilmiş; F03/F04/F05/F08’in beklentileri daraltılmıştır.

Uygulama kaynak kodu değiştirilmemiş, commit veya yayın yapılmamıştır. Oluşturulan dosyalar bu rapor, [ayrıntılı teknik doküman](TEKNIK-DOKUMAN.md) ve [kanıt/yeniden üretim paketi](kanitlar/README.md) ile sınırlıdır.

## 2. Yöntem, kapsam ve güven sınırları

### 2.1 Uygulanan yöntem

1. Başlangıç Git durumu ve commit tespit edildi; çalışma ağacı başlangıçta temizdi.
2. `AGENTS.md` ve GitNexus exploring/CLI becerileri okundu. Kavram sorguları ve named-symbol context üzerinden akışlar izlendi; source ve testler bu yollar üzerinden okundu.
3. GitNexus indeksinin commit'i aynı olmasına rağmen analyzer ortamı değiştiği için bayat işaretlendiği görüldü. `analyze --index-only` ile yenilendi.
4. Manifest, core belge/yazma/ağaç/collation, üç adapter'ın önemli yolları, facade sorgu/indeks/plugin, checkpoint ve live replication, view motoru, sunucu route/güvenlik/lifecycle, CLI/migration ve workflow'lar incelendi.
5. Workspace testleri, Clippy/format, isolated CouchDB 3.5.1 entegrasyonu ve PouchDB T1–T9 çalıştırıldı.
6. Şüpheli davranışlar uygulama koduna dokunmadan ayrı HTTP ve Rust deneyleriyle tekrar üretildi. Uygun olanlarda gerçek CouchDB ile karşılaştırıldı; eşdeğer bulunan davranış hata listesinden çıkarıldı.
7. Kullanıcının online doğrulama isteği üzerine önce resmî belgeler ve sürüme sabitlenmiş CouchDB 3.5.1/PouchDB 9.0.0 kodu okundu; sonra ek upstream deneyleri yürütüldü. Son hükümler bu kaynaklarla güncellendi. Bu turda RouchDB’nin geniş suite’leri tekrar çalıştırılmadı.

### 2.2 Kod ve indeks kapsamı

Workspace `crates/*/src/**/*.rs` altında **58 kaynak dosyası / 37.469 satır**, `crates/*/tests/**/*.rs` altında **58 entegrasyon test dosyası / 29.468 satır** içerir. Kaynak satır sayısı inline testleri de içerir; gerçek production-only LOC veya coverage oranı değildir. Tüm dosyaların hash envanteri [source-manifest.json](kanitlar/source-manifest.json) içindedir. Envanterde bulunmak her satırın manuel gözden geçirildiği anlamına gelmez.

| Alan | Yapılan inceleme | Sınır |
|---|---|---|
| Core | Metadata, revision parse, yazma planları, merge/traversal, sayı karşılaştırma ve JSON derinliği | Tüm olası ağaç kombinasyonlarının matematiksel ispatı yapılmadı |
| Memory/redb | CRUD/bulk, değişiklik akışı, purge/destroy, tablo şeması, transaction/bildirim, attachment ve upgrade kritik yolları | Gerçek disk bozulması, elektrik kesintisi ve kapasite testi yok |
| HTTP adapter | Setup, kimlik, bulk, changes, bulk_get, hata eşleme ve cookie auth | İnternet/proxy/TLS sertifika matrisi test edilmedi |
| Facade/query/views | Find, indeks cache'i, selector/collation, plugin, partition, artımlı view | Tüm operatörlerin tüm değerlerle çapraz ürün testi yok |
| Replikasyon | Checkpoint, başarısız batch sınırı, denied, reset/cancel, event sayaçları | Ağ bölünmesi/çok uzun süreli chaos testi yok |
| Server | Route ağacı; auth/Host/CORS/body; document/local/index/changes/lifecycle/task yolları | Tarayıcı/Fauxton UI ve gerçek reverse proxy kullanılmadı |
| CLI/upgrade | Komut sözleşmeleri, mevcut dosya açma, import/dump/replicate ve backup yükseltme yolları | Üretim verisi veya mevcut kullanıcı DB dosyası açılmadı |
| Test/CI | Yerel suite çalışmaları, workflow ve PouchDB runner incelemesi | Uzak GitHub Actions, branch protection ve coverage yüzdesi doğrulanmadı |

GitNexus yenilemesi **5.802 node, 20.179 edge, 263 cluster, 510 flow** üretti. Ancak analyzer logu flow çıkarımında truncation ve Rust dinamik çağrı çözümleme sınırları bildiriyor. Bu nedenle grafikte görünmeyen yol “etkilenmez” diye yorumlanmamıştır. `Database` ve `put_doc` impact çıktılarındaki **UNKNOWN** doğrudan kaynak/route kayıtlarıyla tamamlanmıştır.

Önemli blast radius: `run_replication` için **CRITICAL, 56 etkilenen sembol / 24 süreç / 2 modül** raporlandı. Önerilen F06 düzeltmesi bu ortak çekirdeğe dokunursa geniş regression kontrolü gerekir. `compare_numbers` için grafikte LOW/1 görünmesi uygulamanın gerçek veri riskini düşürmez: source'da `collate` Mango karşılaştırmalarından ve indeks aralıklarından kullanılır. Grafiğin risk'i ile bulgunun önem seviyesi farklı ölçülerdir.

Bu çalışmada fonksiyon değişikliği veya commit yapılmadığından commit öncesi `detect_changes` bir yayın kapısı olarak kullanılmamıştır. Gelecek düzeltmelerden önce hedef sembollerin impact'i yeniden çalıştırılmalı, commit öncesi tam graph change analizi yapılmalıdır.

### 2.3 Kanıt sınıfları ve öncelik

**Doğrulandı:** mevcut checkout'ta çalıştırılarak gözlendi. **Kaynakla doğrulandı:** koddan doğrudan okunabilen özellik/sınır; runtime etkisinin büyüklüğü ölçülmedi. **Öneri:** sonraki değişiklik için tasarım seçeneği; mevcut implementasyon diye sunulmaz.

- **P1:** sessiz veri kaybı veya yanlış/eksik sorgu sonucu; üretim kullanım yoluna göre yüksek etki.
- **P2:** işlevsel tutarsızlık, güvenilirlik veya başarısızlığın başarı gibi raporlanması; belirli tetik koşulu var.
- **P3:** düşük kapsamlı doğrulama, metadata doğruluğu veya güven sertleştirme sorunu.

Seviyeler CVSS skoru değildir. Güvenlik, doğruluk ve işletim etkileri birlikte değerlendirilmiştir. Mevcut olmayan bir saldırı zinciri veya üretimde gerçekleşmiş veri kaybı varsayılmamıştır.

## 3. Bulgu özeti

| Kimlik | Öncelik | Başlık | Kanıt |
|---|---|---|---|
| F01 | P1 | PUT tombstone gövdesindeki kullanıcı alanları kayboluyor | HTTP deney + CouchDB karşılaştırması |
| F02 | P1 | Arbitrary-precision sayılar Mango eşitliğinde birleşiyor | Rust deney + CouchDB karşılaştırması |
| F03 | P1 | Paylaşılan adapter reset'i diğer handle'ın Mango indeksini bayat bırakıyor | Rust deney |
| F04 | P2 | İndeks kalıcı tanımı ile çalışan RAM tanımı ayrışıyor | HTTP deney + `_index`/`_explain` |
| F05 | P2 | Input'tan gelen sayaçlarda unchecked toplama panic üretiyor | Üç HTTP deney + panic logları |
| F06 | P2 | `batch_size=0` belge taşımadan başarı raporluyor | Rust deney |
| F07 | Öneri | Forwarded Location CouchDB ile uyumlu; isteğe bağlı güven sertleştirme | Online kaynak + iki sunucuda deney |
| F08 | P3 | Purge state değişirken DB info `purge_seq` sabit kalıyor | HTTP deney + opaque-state sözleşmesi |
| F09 | Uyum farkı | Boş alan listesini upstream kabul ederken RouchDB HTTP reddediyor | CouchDB/PouchDB kaynakları ve deney |

Bu tablo ilk dokuz kimliği izlenebilirlik için korur; toplamı dokuz aktif hata değildir. F04 indeks kimliği/tutarlılığı, F09 API kabul kapsamı ile ilgilidir. F05'in üç örneği tek aritmetik sınır bulgusudur.

## 4. Ayrıntılı bulgular

### F01 — P1 — PUT ile silmede tombstone alanları kayboluyor

**Durum:** Çalıştırılarak doğrulandı; CouchDB 3.5.1 ve PouchDB 9.0.0 karşılaştırması var. [Resmî kaynak ve online hüküm](ONLINE-KAYNAK-DOGRULAMASI.md#f01--tombstone-gövdesi-doğrulandı).

**Kaynak:** `crates/rouchdb-server/src/routes/document.rs:235–251`, `crates/rouchdb/src/lib.rs:609–628`.

HTTP handler `_deleted=true` görünce request gövdesini yazmak yerine `Database::remove(id, rev)` çağırıyor. `remove`, `data: {}` ile yeni silinmiş belge üretiyor. Bu davranış normal DELETE için uygundur; kullanıcı gövdesi taşıyan PUT'ta alanları sessiz atar.

**Yeniden üretim:** `http-probes.py` içindeki `tombstone_delete` ve `tombstone_stored_body` kayıtları. Önce belge yaratılır; ardından:

```json
{"_rev":"<güncel rev>","_deleted":true,"kind":"order","tenant":"A","deleted_at":"2026-11-14"}
```

PUT 201 döner. Yeni revision açıkça istenerek okunduğunda RouchDB yalnız `_id`, `_rev`, `_deleted` döndürür. Aynı deneyde CouchDB `kind`, `tenant`, `deleted_at` alanlarını da korur. [RouchDB kanıtı](kanitlar/http-probes.json), [CouchDB kanıtı](kanitlar/couchdb-reference-probes.json).

**Etki:** Silme sebebi/zamanı veya tenant gibi tombstone üzerindeki uygulama metadata'sı kaybolur. Bu alanlara dayalı changes/replication selector'ları silmeyi farklı değerlendirebilir. Tüm replikasyonlarda silmelerin kaybolduğu iddia edilmez; alan bazlı filtre kullanan yol etkilenebilir.

**Önerilen düzeltme:** DELETE'in semantiğini koruyarak PUT silmesini tam body üzerinden `put`/`update` veya eşdeğer tek-belge yazma yoluna taşıyın. `_id`, rev kaynaklarının tutarlılığı, attachment'ın tombstone'dan atılması ve plugin doğrulaması korunmalı.

**Kabul testi:** PUT tombstone'a eklenen metadata hem explicit-rev GET hem `_changes?include_docs=true` hem karşı tarafa replication sonrası korunmalı; normal DELETE gövdesi ve conflict davranışı değişmemeli.

### F02 — P1 — Arbitrary-precision saklama hassasiyeti sorguda korunmuyor

**Durum:** Feature açık Rust programıyla ve CouchDB karşılaştırmasıyla doğrulandı. PouchDB JavaScript Number kullandığından sınırsız hassasiyet referansı değildir. [Online kaynak değerlendirmesi](ONLINE-KAYNAK-DOGRULAMASI.md#f02--büyük-tamsayı-eşitliği-couchdb-açısından-doğrulandı).

**Kaynak:** `crates/rouchdb-core/src/collation.rs:126–145`; Mango koşullarında `collate` kullanımı `crates/rouchdb-query/src/mango.rs:731–745`; mevcut büyük sayı testleri `crates/rouchdb/tests/big_numbers.rs:1–93`.

`as_i128` adı geniş integer desteği izlenimi verse de yalnız `as_i64`/`as_u64` okur. İkisi de başarısız olunca `compare_numbers`, değerleri f64'e dönüştürür. `arbitrary-precision` açıkken ayrı saklanan `18446744073709551616` ve `18446744073709551617` aynı f64'e yuvarlanır.

**Yeniden üretim:** `library-probes --features arbitrary-precision`, `big_integer_eq` kaydı. İki belge ayrı sayıları taşır. `$eq:18446744073709551616` sorgusu RouchDB'de **iki belge**, CouchDB'de **yalnız `one`** döndürür. [Rust kanıtı](kanitlar/library-probes-arbitrary-precision.jsonl), [referans](kanitlar/couchdb-reference-probes.json).

**Etki:** Eşitlik, aralık, sıralama ve aynı comparator'ı kullanan indeks seçimleri yanlış olabilir. Selector tabanlı replication aynı sorgu motorunu kullandığından filtre doğruluğu da etkilenebilir. Varsayılan modda parse anında zaten f64'e yuvarlanan sayılarla bu feature hatası birbirinden ayrılmalıdır.

**Önerilen düzeltme:** Arbitrary-precision değerler için decimal/integer metninden tam karşılaştırma veya uygun arbitrary precision sayısal temsil kullanın. İndeks ve filtre aynı comparator'ı kullanmalı. “Saklama tamdır, karşılaştırma yaklaşık” şeklinde ürün sınırı seçilecekse açıkça belgelenmeli; mevcut exact-integer sorgu beklentisi karşılanmaz.

**Kabul testi:** Komşu >u64 tamsayılar, negatif alt sınır, büyük exponent, hassas decimal ve integer/float eşdeğerliği; indexed/unindexed find, reduce grup anahtarı ve replication selector üzerinde test edilmeli. Mevcut round-trip testlerinin geçmesi bu kusuru yakalamıyor.

### F03 — P1 — Ortak storage reset'i diğer handle'ın indeksini geçersizleştirmiyor

**Durum:** İki yasal `Database::from_adapter` handle'ıyla memory üzerinde deterministik tekrarlandı. Bu, RouchDB'nin yeniden kullanılabilir handle sözleşmesine göre hatadır; PouchDB eski handle'ı aynı biçimde yeniden kullanmaz. [Online yaşam döngüsü kıyası](ONLINE-KAYNAK-DOGRULAMASI.md#f03--reset-sonrası-indeks-rouchdb-yaşam-döngüsü-hatası).

**Kaynak:** `crates/rouchdb/src/lib.rs:480–487,865–901,1233–1240`. Aynı storage üzerinde başka handle kullanımı proje fixture'ında da vardır: `crates/rouchdb-bench/src/lib.rs`, `Fixture::another_handle`.

Her facade kendi `MangoIndex.last_seq` değerini tutar. `destroy()` yalnız çağrılan handle'ın indekslerini temizler. Diğer handle'ın `index_candidates` yolu adapter kimliğini veya reset'i kontrol etmeden eski `since` ile changes okur; değişiklik boşsa eski adayları kullanır.

**Yeniden üretim:** Ortak `Arc<MemoryAdapter>` → A ve B facade'ları. A ile `old:{x:1}` yazıp x indeksi yaratın; sequence 1 olsun. B ile destroy edip `new:{x:1}` yazın; yeni sequence tekrar 1 olur. A'nın indexed sorgusu **[]**, B'nin tam taraması **[new]** döner. [Kanıt](kanitlar/library-probes.jsonl), `shared_handle_reset`.

**Etki:** Veri storage'da mevcutken indeksli sorgudan sessizce kaybolur. Tek handle ile `Database::destroy()` kullanıldığında indeks temizlenir; bulgu bütün destroy çağrılarını kapsamaz. redb için aynı algoritmik risk source'da vardır ancak bu özel reset deneyi yalnız memory backend'de yürütülmüştür.

**Önerilen düzeltme:** Mango cache'ini adapter kimliği/reset generation'ıyla ilişkilendirin; değişince rebuild yapın. Reset'ten önce/sonra aynı seq'e ulaşılması da ele alınmalı, yalnız `update_seq < last_seq` kontrolü yeterli değildir. `ViewEngine`'in reset yaklaşımı tasarım örneği olabilir.

**Kabul testi:** Memory/redb ortak handle reset, daha düşük/eşit/daha yüksek yeni sequence ve reset sonrasında eski ID'nin farklı içerikle tekrar yazılması; indexed sonuçlar her zaman full scan'le aynı olmalı.

### F04 — P2 — İndeksin persisted ve RAM tanımları ayrışıyor

**Durum:** HTTP üzerinden doğrulandı. CouchDB replacement yaparken PouchDB aynı `(ddoc,name)` için eski tanımı koruyabilir; iki modelde de metadata/çalışan tanım tutarlıdır. [Online kaynak ve deney](ONLINE-KAYNAK-DOGRULAMASI.md#f04--indeks-kimliği-ve-yeniden-tanım-doğrulandı-çözüm-seçenekleri-düzeltildi).

**Kaynak:** `crates/rouchdb-server/src/routes/query.rs:219–263,293–299`; `crates/rouchdb/src/lib.rs:938–946,961–970`.

HTTP önce design document'ı yeni alanlarla yazar, ardından facade `create_index` çağırır. Facade cache'i yalnız `name` ile anahtarlanır; bu isim varsa ddoc ve fields karşılaştırılmadan `exists` döner. Aynı isim farklı ddoc altında da çakışır.

**Deney:** `same` adında `_design/first` içinde x indeksi yaratıldı; `_design/second` içinde y için aynı isim gönderildi. `_index` iki tanımı listelerken `_explain(y)` full scan dedi. Ardından first/same tanımı x'ten y'ye değiştirildi; `_index` y gösterirken `_explain(x)` hâlâ first/same x indeksini kullandığını gösterdi. [Kanıt](kanitlar/http-probes.json): `index_y_same_name`, `indexes_after_replace`, `explain_x_after_replace`.

**Etki:** Gösterilen tanım yürütülen planı açıklamaz, restart sonrası rebuild davranışı değişebilir, isim bazlı silme diğer ddoc'un cache'ini etkileyebilir. Bu deneyde yanlış belge sonucu kanıtlanmadı; stale indeks tanımı nedeniyle her sorgunun veri kaybettiği iddia edilmez.

**Önerilen düzeltme:** İndeks kimliğini `(ddoc,name)` olarak modelleyin. Yeniden tanımda CouchDB gibi tutarlı replacement veya PouchDB gibi eski tanımı bütünüyle koruyan no-op seçilebilir. Persisted tanım ile cache ayrı davranmamalı; başarı yanıtı gerçek çalışan tanımı yansıtmalı.

**Kabul testi:** Aynı isim/farklı ddoc, aynı kimlik/farklı alanlar, delete ve restart; `_index`, `_explain` ve gerçek sorgu sonuçları tutarlı olmalı.

### F05 — P2 — Büyük sayaçlar unchecked aritmetik üzerinden panic üretiyor

**Durum:** Debug derlemede üç ayrı istekle doğrulandı. Release binary üzerinde bu deney yapılmadı. CouchDB büyük revision değerlerini kabul edip artırabiliyor; bunlar protokol açısından zorunlu olarak geçersiz değildir. [Online sınır değerlendirmesi](ONLINE-KAYNAK-DOGRULAMASI.md#f05--aritmetik-sınırları-doğrulandı-geçersiz-input-anlatımı-düzeltildi).

| Yol | Kaynak | Tetik | Gözlenen sonuç |
|---|---|---|---|
| `_find` | `routes/query.rs:73` | Geçerli bookmark içindeki skip=u64::MAX + request skip=1 | Bağlantı JSON cevap gelmeden kapandı, worker panic |
| `_local` PUT | `routes/local.rs:75–79` | `_rev:0-18446744073709551615` | Bağlantı kapandı, worker panic |
| Normal belge edit'i | `core/src/write.rs:140` | `new_edits=false` ile MAX generation seed; sonra update | redb blocking task panic; HTTP 500 ve iç hata metni |

İlk iki yol `crates/rouchdb-server/src/`, son yol `crates/rouchdb-core/src/` altındadır. [Tam request sonuçları ve panic logları](kanitlar/http-probes.json): `bookmark_skip_overflow`, `local_revision_overflow`, `max_revision_seed`, `max_revision_edit`.

**Etki:** RouchDB'nin sayısal temsil sınırını aşan input/hesap kontrollü hata veya geniş temsil yerine panic/500 üretir. Input'un CouchDB'de geçersiz olduğu varsayılmaz. İlk panic sonrası health/info 200 verdi; **tüm sunucunun çöktüğü iddia edilmez**. Release davranışı overflow-check/profile ayarına bağlıdır ve çalıştırılmadan aynı sonuç garantilenemez. Auth açıksa bu DB uçlarına erişim yine credentials gerektirir.

**Önerilen düzeltme:** Daha geniş temsil veya `checked_add` ile açık destek sınırı ve kontrollü hata dönüşü; revision maksimumunun kabul/ret sözleşmesi. Sayfalama sonucuna doküman sayısı eklenmesi de kontrol edilmeli. Sadece saturating toplama, geçersiz revision/offset'i doğru değer gibi kabul edebilir.

**Kabul testi:** 0, MAX-1, MAX sınırları; kontrolsüz panik yok, JSON 4xx, aynı DB sonraki geçerli isteklerde çalışır. Debug ve release modunda aynı input sözleşmesi.

### F06 — P2 — Sıfır batch boyutu replikasyonu başarılı no-op yapıyor

**Durum:** Memory→memory Rust deneyiyle doğrulandı. PouchDB 9.0.0 karşı deneyinde sıfır 100’e normalleşip belge taşındı. [Online kaynak ve deney](ONLINE-KAYNAK-DOGRULAMASI.md#f06--sıfır-batch-pouchdb-karşılaştırmasıyla-güçlendi).

**Kaynak:** `crates/rouchdb-replication/src/protocol.rs:250–270,432–452,628–635`; `crates/rouchdb-adapter-memory/src/lib.rs:463–467`.

`ReplicationOptions.batch_size` sıfır olarak doğrulanmıyor. Changes `limit=0` boş döndüğünde replikasyon bunu kaynak tükenmiş gibi ele alıyor ve error olmadığı için `ok=true` üretiyor.

**Kanıt:** Kaynakta bir belge varken `batch_size:0` ile sonuç `ok=true, docs_read=0, docs_written=0, target_count=0`. [Kanıt](kanitlar/library-probes.jsonl): `zero_batch`.

**Etki:** Yanlış parametre, işlemin başarılı olduğu izlenimiyle veri taşımayı durdurur. Fiziksel veri silinmesi değildir; başarılı replikasyon sözleşmesi sorunudur. Varsayılan batch 100 etkilenmez.

**Önerilen düzeltme:** ReplicationOptions için ortak validation/normalleştirme. PouchDB ile birebir uyum hedefleniyorsa sıfırı 100’e normalleştirin; BadRequest seçeneği bilinçli bir API ayrışması olur. Tek geçiş, event'li ve live entrypoint'leri aynı doğrulamayı kullanmalı. `run_replication` impact'i CRITICAL olduğundan değişiklik dar tutulmalı ve tüm replikasyon suite'i yeniden çalıştırılmalı.

**Kabul testi:** 0 boyut kontrollü hata, 1 ve varsayılan boyutta aynı belgeler; zero live opts sonsuz başarı/poll döngüsü üretmemeli, checkpoint yanlış ilerlememeli.

### F07 — Sertleştirme önerisi — Forwarded Location CouchDB ile uyumlu

**Hüküm:** Önceki aktif hata sınıflandırması geri çekildi. RouchDB'de gözlenen başlık etkisi gerçek, fakat CouchDB de aynı varsayılan davranışı uyguluyor.

**Yerel kaynak:** `crates/rouchdb-server/src/routes/mod.rs:63–86`. Cookie'deki `trust_proxy` ayarı ayrı bir mekanizmadır; bu ayarın mevcut sözleşmesi Location başlıklarını yönettiğini garanti etmez.

Resmî HTTP ayarları forwarded host'un Host'tan öncelikli olduğunu açıklar. CouchDB 3.5.1 `absolute_uri` kodu ve yeni gerçek HTTP deneyi bunu doğruladı: her iki sunucuda da forwarded başlığı `https://untrusted.invalid/...` Location üretebiliyor. [Resmî ayarlar](https://docs.couchdb.org/en/3.5.1/config/http.html#chttpd/x_forwarded_host), [sabit kaynak ve değerlendirme](ONLINE-KAYNAK-DOGRULAMASI.md#f07--forwarded-location-hata-iddiası-geri-çekildi), [CouchDB deney kaydı](kanitlar/upstream-reference-probes.json).

**Kapsam:** Bir 201 yanıtındaki URL etkisi tek başına 3xx redirect, SSRF, credentials sızıntısı veya auth bypass kanıtı değildir. Bu zincirlerin hiçbiri gösterilmedi.

**İsteğe bağlı öneri:** Deployment gerektiriyorsa forwarded başlıklarını yalnız güvenilir proxy'den kabul edin veya relative Location seçin. Bu tercih CouchDB varsayılanından bilinçli ayrışma olur; zorunlu uyumluluk düzeltmesi değildir. F07 yedi aktif davranış hatasına dahil değildir.

### F08 — P3 — DB info purge durumunu değişmeden gösteriyor

**Durum:** Yerel gözlem korunuyor; önceki sayısal sayaç/eşitlik beklentisi geri çekildi.

**Kaynak:** `crates/rouchdb-server/src/routes/database.rs:20–39` içindeki sabit `purge_seq:0`; yerel storage state `crates/rouchdb-adapter-redb/src/lib.rs:168,1773–1777` içinde değişiyor.

CouchDB DB info sözleşmesi `purge_seq` değerini **opak purge durumu belirteci** olarak tanımlar; işlem sayısı çıkarılmamalıdır. Clustered `_purge` yanıtında null dönebilir. Bu nedenle `_purge.purge_seq` ile DB info alanının birbirine sayısal eşitliği doğru kabul ölçütü değildir. [Resmî DB info sözleşmesi](https://docs.couchdb.org/en/3.5.1/api/database/common.html#db), [sürümü sabit kaynak analizi](ONLINE-KAYNAK-DOGRULAMASI.md#f08--purge-state-sorun-korundu-sayaç-eşitliği-beklentisi-kaldırıldı).

**Deney:** Yeni CouchDB 3.5.1 çalışmasında purge öncesi/sonrası DB info belirteci değişti; `_purge` cevabı null verdi. RouchDB'de storage purge state değişmesine rağmen info hep 0 üretiyor. [RouchDB kanıtı](kanitlar/http-probes.json), [yeni CouchDB kanıtı](kanitlar/upstream-reference-probes.json).

**Etki:** DB info üzerinden purge state değişimini gözleyen istemci bu değişimi algılayamaz. Ayrıca disk/data size alanları da ölçüm olmayan placeholder'lardır. Bu bulgu purge edilen verinin storage'da kaldığını göstermez.

**Önerilen düzeltme:** Yerel sayacı istemci için aritmetik sözleşmeye dönüştürmeden, DB info'da gerçek purge state'i temsil edin. Clustered CouchDB'nin opak token biçimini taklit etmek zorunlu değildir; sabit ve yanlış state bildirmemek gerekir.

**Kabul testi:** Gerçek purge sonrası DB info belirteci state değişimini yansıtmalı; restart'ta doğru kalmalı. Belirtecin sayısal toplam veya `_purge` cevabıyla eşit olduğu varsayılmamalı.

### F09 — API uyum farkı — HTTP boş indeks alan listesini reddediyor

**Hüküm:** “Rust API boş indeksi kabul ettiği için hatalı” gerekçesi ve ortak katmanda zorunlu ret önerisi geri çekildi.

**Kaynak:** Rust facade `crates/rouchdb/src/lib.rs:915–918` boş listeyi kabul eder; HTTP `crates/rouchdb-server/src/routes/query.rs:279–283` bunu 400 ile reddeder.

Resmî CouchDB 3.5.1 validator yolu boş listeyi kabul ediyor. PouchDB 9.0.0 da bunu reddetmiyor. Yeni referans deneylerinin **ikisinde de** `fields:[]` için `created` alındı. Dolayısıyla Rust tarafının kabulü upstream ile uyumludur. [Sabit kaynaklar ve online değerlendirme](ONLINE-KAYNAK-DOGRULAMASI.md#f09--boş-indeks-önceki-validation-gerekçesi-geri-çekildi), [CouchDB deneyleri](kanitlar/upstream-reference-probes.json), [PouchDB deneyleri](kanitlar/pouchdb-upstream-probes.json).

**Kalan fark:** RouchDB HTTP katmanı aynı tanımı kabul etmiyor. Bu, kabul edilen API kapsamı farkıdır; boş indeks kabulünden doğan yanlış belge sonucu kanıtlanmadı. Yedi aktif davranış hatasına dahil edilmedi.

**Öneri:** Tam API uyumu hedefleniyorsa HTTP kabulünü ve indeks listeleme/restore yolunu upstream ile eşleyin. Dar kapsam bilinçli seçilecekse fark açıkça belgelenebilir. Rust API'ye zorunlu ret eklemek online kaynaklarla doğrulanan bir gereklilik değildir.

## 5. Eksikler ve kaynakla belirlenen işletim riskleri

Bu bölümdeki maddeler yukarıdaki 7 davranış hatasına eklenmemiştir; F07 öneri ve F09 uyum farkı da ayrı değerlendirilir. Bazıları bilinçli ürün sınırıdır; bazıları ölçüm veya gereksinim kararı gerektirir.

### G01 — PouchDB senaryoları CI kapısına bağlı değil

`run-tests.sh` T1–T9'u düzgün biçimde geçici DB ve ephemeral port'larda çalıştırır. Fakat `.github/workflows/` dosyalarında PouchDB kurulumu/runner çağrısı yoktur; `.github/workflows/ci.yml` Rust/CouchDB testlerini içerir. Yerelde geçen fork'a özgü JavaScript uyumluluğu sonraki değişiklikte CI tarafından korunmayabilir. Öneri: `npm ci`, server build, `ROUCHDB_SERVER_BIN` ile runner ve sonuç artifact'i eklemek. T4 yaklaşık 90 saniye, T8/T9 ayrıca süre gerektirir; job timeout buna göre seçilmeli.

### G02 — Büyük DB'de sorgu ve pending hesapları pahalı olabilir

`mango.rs:489–505` full scan sorguda belgeleri toplar. `Database::index_candidates` changes'i limitsiz okuyabilir; `BuiltIndex::apply_changes` tüm indeks girişlerini yeniden sıralar (`mango.rs:240–296`). Sunucu `_changes` sayfası doluysa kalanların sayısını bulmak için tekrar limitsiz changes çağırır (`routes/changes.rs:274–300`). N belgeyi b boyutlu sayfalarla tüketirken pending taramalarının toplam işi yaklaşık N + (N-b) + ... ölçeğine çıkabilir.

Bu kod yollarının varlığı doğrulandı; **üretimde bellek taşması veya belirli latency eşiğinin aşıldığı ölçülmedi**. Öneri: gerçek veri profiliyle benchmark, count-only pending yolu, iterator/stream sorgu, bounded indeks güncelleme ve replikasyon p95/p99 ölçümleri. Limit parametresi tek başına belleğe alınan veri boyutunu sınırlamıyor.

### G03 — Güvenlik modeli tek admin; security document ACL motoru değil

`Auth` yalnız yapılandırılmış admin'i doğrular; `_security` endpoint'i belgeyi alır/yazar (`routes/security.rs:11–37`). Auth middleware'i security üyelerini değerlendirmez (`auth.rs:283–332`). Bu nedenle çok kullanıcılı CouchDB ACL, tenant izolasyonu veya `_users` üyeliği bu sunucunun mevcut kabiliyeti değildir.

Auth kapalı yerel sunucuda security document'a member yazmak erişimi kısıtlamaz; bunu sağlayacağı varsayılmamalıdır. Bu denetim gerçek bir credentials bypass kanıtlamadı. Brute-force rate limiting/session sayısı kotası da uygulama güvenlik katmanında görünmüyor; non-loopback kurulumda reverse proxy kontrolü ve uygulama gereksinimleriyle ele alınmalı. Public auth disabled ayarının bilinçli seçimi ayrıca bir zafiyet diye sayılmadı.

### G04 — Capability ilanı uygulama sınırlarından geniş

`routes/root.rs:14–20` CouchDB 3.3.3 emülasyonunu ve `partitioned`, `reshard`, `scheduler` gibi feature isimlerini ilan ediyor. `routes/mod.rs:130–245` bu cluster/partition API'lerini kaydetmiyor. JavaScript view endpoint'leri de var fakat çalıştırma yapmıyor (`routes/views.rs:30–81`).

Öneri: gerçek yetenek matrisi yayınlayın; root capability listesini gerçekten desteklenen davranışla eşleyin. Rust `Partition` desteği HTTP partition API desteği olarak sunulmamalı. Sürüme bakarak tam CouchDB desteği varsayan istemciler için ürün entegrasyon testi eklenmeli.

### G05 — Convenience API'leri bazı uzak hataları sessizleştiriyor

`Database::get_indexes` remote error'ı `unwrap_or_default` ile boş listeye çevirir (`rouchdb/src/lib.rs:981–993`). `explain` remote cevap yoksa yerel plan üretebilir (`:1016–1046`). Bu davranışlar API dokümantasyonunda kısmen belirtilir; yine de “indeks yok” ile “sunucu erişilemiyor” ayrımını kaybettirir.

Öneri: mevcut convenience API'yi kırmadan `try_get_indexes`/`try_explain` gibi Result döndüren alternatifler ve structured diagnostics. İzleme sistemleri bu fallback çıktısını doğrulanmış remote plan sanmamalı.

### G06 — Rehberde birkaç operasyonel bilgi güncel değil

`docs/book/src/guides/http-server.md:67` longpoll varsayılanını 30.000 ms yazar; source `routes/changes.rs:22` **60.000 ms** kullanır. Aynı rehberin Fauxton adımları asset indirdikten sonra rebuild gerektiğini göstermiyor (`:12–22`); asset'ler `rust_embed` ile build'e alınır ve source placeholder bu gerekliliği açıkça söyler (`routes/fauxton.rs:4–7,45–54`). `guides/replication.md` sync'i “at once” anlatırken `Database::sync` push ardından pull çalıştırır (`rouchdb/src/lib.rs:1198–1202`).

Öneri: eski anlatımların yerini mevcut davranış alsın. Kaynakta doğru olan bilgiye sonradan çelişkili düzeltme notları yığmak yerine ana rehber metni düzeltilmeli. Bu denetim kapsamında mevcut rehberler değiştirilmedi; yeni teknik doküman güncel davranışı açıklar.

## 6. Doğrulama sonuçları

| Kontrol | Sonuç | Kanıt |
|---|---|---|
| `cargo fmt --all -- --check` | Exit 0 | `kanitlar/fmt.log` — başarıda boş olabilir |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Exit 0 | `kanitlar/clippy.log` |
| `cargo test --workspace --locked --no-fail-fast` | 1006 başarılı, 0 hatalı, 130 ignored | `kanitlar/tests.log` |
| Aynı suite + `--features rouchdb/arbitrary-precision` | 1006 başarılı, 0 hatalı, 130 ignored | `kanitlar/arbitrary-precision.log` |
| `scripts/test-couchdb.sh --locked` | 130 başarılı, 0 hatalı; 986 normal test filtrelendi | `kanitlar/couchdb.log` |
| CouchDB büyük sayı round-trip + feature | 1 başarılı | `kanitlar/couchdb-numbers.log` |
| PouchDB T1–T9 | 9/9 GEÇTİ; runner exit 0 | `kanitlar/pouchdb/t*.json`, `pouchdb-runner.log` |
| Denetime özgü Rust deneyleri | F02/F03/F06/F09 davranışları tekrarlandı | `library-probes*.jsonl` |
| Denetime özgü HTTP deneyleri | F01/F04/F05/F07/F08 tekrarlandı | `http-probes.json` |
| Referans CouchDB deneyleri | Tombstone ve sayı farkı doğrulandı; nil bookmark eşdeğer | `couchdb-reference-probes.json` |

Online kontrol turunda ayrıca [yeni CouchDB istekleri](kanitlar/upstream-reference-probes.json) ve [PouchDB 9.0.0 deneyleri](kanitlar/pouchdb-upstream-probes.json) tamamlandı; upstream kaynaklar [hash manifestinde](kanitlar/online-source-manifest.json) sabitlenmiştir. Yukarıdaki geniş RouchDB testleri önceki denetim koşusudur, bu turda tekrar yürütülmemiştir.

Sayılar test binary'lerinin `test result` satırlarından toplanmıştır; workspace satırı unit/integration/doctest toplamıdır. İki feature çalışmasındaki aynı testler iki ayrı çalışmadır; toplamı benzersiz test sayısı değildir. Özet makine tarafından [validation-summary.json](kanitlar/validation-summary.json) dosyasına da yazılmıştır.

CouchDB testleri mevcut servise yöneltilmedi: özel container, loopback'e publish edilen rastgele port ve test verileri kullanıldı. HTTP deneyleri ayrı temporary redb dosyaları kullandı; PouchDB runner her senaryoda ayrı temporary DB açtı. İnceleme sonunda denetime ait container kapatıldı. Kullanıcının mevcut veritabanları açılmadı veya yükseltilmedi.

Denetime özgü Rust programındaki assertion'lar **kusurun mevcut olduğunu** doğrular; bu programların exit 0 olması ürünün doğru olduğu anlamına gelmez. Bunlar düzeltme sırasında normal regression beklentilerine çevrilmelidir. HTTP deneyleri yanıtları ve panic loglarını kaydeder; burada sunulan 4xx/500 sonuçlarının amacı mevcut hatayı görünür kılmaktır.

Çalıştırılmayanlar: Windows/MSRV/TLS kombinasyon matrisi, Criterion performans ölçümü, mutation testi, coverage ölçümü, dependency advisory taraması, gerçek TLS proxy/tarayıcı/Fauxton, uzun süreli chaos ve disk doluluk/elektrik kesintisi deneyleri. Bunlar için temiz sonuç veya belirli coverage yüzdesi iddia edilmez.

## 7. Hata olarak kabul edilmeyen şüpheler

| Davranış | Neden hata listesinde değil? |
|---|---|
| Forwarded başlıklarının Location üretiminde kullanılması | CouchDB 3.5.1 aynı varsayılanı uygular; tek başına hata/zafiyet sayılmadı. |
| Rust API'nin `fields:[]` kabulü | CouchDB ve PouchDB de kabul ediyor; kalan fark HTTP'nin daha dar kabulüdür. |
| Boş `_find` sonucu `bookmark:"nil"` veriyor, tekrar gönderince 400 oluyor | Aynı davranış gerçek CouchDB 3.5.1'de yeniden üretildi. |
| Varsayılan testlerde 130 ignored var | Ayrı CouchDB suite'inde bu 130 test çalıştırılıp geçti. |
| Core trait test double içinde `unimplemented!()` var | Üretim implementasyonu eksikliği değil; Minimal test adapter'ının çalıştırılmayan metotları. |
| Objelerde anahtar sırası CouchDB ile farklı | `reference/differences.md` içinde açık kabul edilmiş fark. |
| String sıralaması ICU ile aynı değil | Core `collate` açıklamasında açık sınır; kabul edilip edilmeyeceği ürün gereksinimine bağlı. |
| JS view çalıştırılamıyor | Kaynak ve belgelerde açıklanan mevcut ürün sınırı; “tüm CouchDB özellikleri” beklentisiyle birlikte değerlendirilmesi gerekir. |
| `after_write` hatasında veri commit edilmiş kalıyor | Plugin sözleşmesinde açık davranış; otomatik rollback garantisi yok. |
| Dump conflict/revision geçmişini almıyor | CLI sözleşmesinde açık; tam kopya için replication öneriliyor. |

## 8. Düzeltme sırası ve bitiş ölçütleri

| Sıra | İş paketi | Bitiş ölçütü |
|---|---|---|
| 1 | F01 tombstone gövdesi | PUT/GET/changes/replication metadata round-trip; CouchDB differential test |
| 2 | F02 tam sayı comparator | Adjacent büyük sayılar ayrı; indexed/unindexed/filter aynı; iki feature suite'i yeşil |
| 3 | F03 reset/indeks kimliği | Memory ve redb ortak-handle reset matrisi full-scan ile aynı sonucu verir |
| 4 | F04 indeks namespace ve replacement | Persisted/RAM/query plan restart ve delete boyunca aynı tanımı gösterir |
| 5 | F05 checked arithmetic | Malformed/MAX input'lar debug/release'te paniksiz 4xx |
| 6 | F06 replication validation | Sıfır batch reddi; one-shot/events/live tutarlılığı; checkpoint regression'ları geçer |
| 7 | F08, F09 uyum kararı ve G06 | Gerçek purge state; boş indeks için upstream uyum kararı; doğru rehber. F07 ayrı ve opsiyonel sertleştirmedir. |
| 8 | G01 CI gate | PouchDB T1–T9 PR'da çalışır, sonuçları artifact olarak tutulur |
| 9 | G02–G05 ürün/işletim | Kapasite hedefi, capability matrisi, admin/ACL sınırı, hatayı saklamayan API kararı |

Uygulama değişikliği yapmadan önce her hedefe GitNexus upstream impact çalıştırılmalı. Özellikle replikasyon çekirdeğinin CRITICAL blast radius'u açıkça dikkate alınmalı. Düzeltme commit'inden önce partial/truncated olmayan graph change analizi, ilgili dar regression testleri ve mevcut geniş suite'ler tamamlanmalı. Bu rapor düzeltmeleri uygulamaz ve açık bulguları kapatılmış saymaz.
