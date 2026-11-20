# Dokuz bulgunun online kaynaklarla doğrulanması

**Okuma ve kontrol tarihi:** 14 Kasım 2026. **Yerel kaynak:** RouchDB `d1259449bb1ad20d2df2626989b3a581b7e7893f`.

Önce resmî belgeler ve upstream kaynak kodu okundu, ardından yalnız açık kalan davranışlar için yeni referans deneyleri yapıldı. Kaynaklar üçüncü taraf özetlerinden alınmadı. CouchDB **3.5.1** etiketi `44f6a43d81b83b203a110f6036d107dc2afb5556`, PouchDB **9.0.0** etiketi `b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1` commit'lerine sabitlendi.

PouchDB release etiketi paketlerin `src/` ağacını içermediği için resmî, küçültülmemiş `dist/pouchdb.js` ve `dist/pouchdb.find.js` okundu. Bu dosyaların kaynak linkleri commit'e sabittir. CouchDB belgelerinin değişebilen `stable` sayfalarına ek olarak aynı 3.5.1 commit'indeki RST kaynakları da okundu. URL ve SHA-256 kayıtları [online-source-manifest.json](kanitlar/online-source-manifest.json) içindedir.

**Sonuç:** İlk dokuz madde artık aynı türden dokuz hata olarak sunulmuyor. **7 davranış hatası korunuyor; F07 sertleştirme önerisine ayrıldı; F09'un eski gerekçesi geri çekilip HTTP API uyum farkı olarak düzeltildi.** F03'ün kıyası, F04'ün replacement önerisi, F05'in input sınırı ve F08'in purge beklentisi daraltıldı. Ana rapor ve teknik doküman bu sonuçlarla güncellendi.

## F01 — Tombstone gövdesi: doğrulandı

CouchDB'nin replikasyon rehberi, gövde alanlarına bağlı filtrelerle silmeleri taşımak için DELETE yerine `_deleted:true` içeren belge yazılmasını açıklar. Bu, alanların korunmasının gerçek bir kullanım sözleşmesi olduğunu gösterir. [Resmî rehber](https://docs.couchdb.org/en/3.5.1/replication/intro.html#controlling-which-documents-to-replicate).

3.5.1 parser'ı silinme işaretini okuyup kalan belge alanlarını işlemeye devam eder; silinme işareti bütün kullanıcı gövdesini boşaltan bir komut değildir. [couch_doc.erl, satır 327 ve devamı](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/couch/src/couch_doc.erl#L327).

Önceki CouchDB HTTP deneyine ek olarak PouchDB 9 memory üzerinde PUT tombstone denendi; `tenant` ve `deleted_at` korundu. RouchDB'nin kaybettiği bu alanlar her iki referansta korunuyor. **F01/P1 devam ediyor.** Bu kontrol attachment gövdesinin bütün silme yollarındaki davranışına ilişkin yeni bir sonuç üretmez.

## F02 — Büyük tamsayı eşitliği: CouchDB açısından doğrulandı

CouchDB sayı açıklaması, integer ile decimal/exponent ve JavaScript sayı yollarını ayırır; özellikle JavaScript ortamında temsil hassasiyeti kaybına dikkat çeker. [Number Handling](https://docs.couchdb.org/en/3.5.1/api/basics.html#number-handling).

PouchDB'nin comparator'ı JavaScript Number aritmetiğini kullanır. **PouchDB büyük tamsayı hassasiyetinin doğrulama otoritesi değildir**: iki komşu >u64 tamsayı JavaScript JSON parse sırasında zaten aynı değere dönüşür. [PouchDB 9 comparator](https://github.com/pouchdb/pouchdb/blob/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.find.js#L2825).

RouchDB `arbitrary-precision` ile iki farklı sayıyı saklayabildiği halde karşılaştırmada birleştiriyor; önceki gerçek CouchDB deneyinde `$eq` yalnız doğru belgeyi getirmişti. **F02/P1, CouchDB karşılaştırması ve RouchDB'nin kendi exact-storage feature'ı kapsamında devam ediyor.** “PouchDB aynı sayıları tam hassasiyetle ayırıyor” iddiası yapılmıyor.

## F03 — Reset sonrası indeks: RouchDB yaşam döngüsü hatası

PouchDB'nin çağrı wrapper'ı closed/destroyed handle'ları reddeder; aynı DB adına ait instance'lara destroy bildirimi dağıtılır. [Handle kontrolü](https://github.com/pouchdb/pouchdb/blob/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.js#L2649), [instance bildirimleri](https://github.com/pouchdb/pouchdb/blob/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.js#L5400).

Yeni PouchDB deneyinde başka handle DB'yi sildikten sonra eski handle `database is closed` hatası verdi; yeniden açılan handle yeni belgeyi buldu. Dolayısıyla “PouchDB eski handle'ı aynı biçimde yeniden kullanır” kıyası kurulamaz.

RouchDB ise `destroy()` sonrasında handle'ın kullanılabilir kalmasını açıkça destekliyor ve `from_adapter` ile ortak storage açıyor. O sözleşme altında indexed/full-scan farkı yerel deneyde mevcut. **F03/P1 korunuyor, ancak doğrudan CouchDB/PouchDB protokol ihlali diye sunulmuyor.**

## F04 — İndeks kimliği ve yeniden tanım: doğrulandı, çözüm seçenekleri düzeltildi

CouchDB indeksleri design document ile ilişkilendirir; list/delete yolları bu kimliği taşır. [İndeks API'si](https://docs.couchdb.org/en/3.5.1/api/database/find.html#db-index). PouchDB kaynak kodu da view adını ilgili ddoc içinde arar. [PouchDB createIndex](https://github.com/pouchdb/pouchdb/blob/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.find.js#L5495).

Yeni deneylerde iki referans da aynı `name` ile farklı ddoc'larda iki indeks yaratabildi. Aynı `(ddoc,name)` farklı alanla yeniden gönderildiğinde **CouchDB tanımı değiştirip `created`**, **PouchDB eski tanımı koruyup `exists`** döndürdü. İki davranış da metadata ile çalışan tanımı tutarlı bırakıyor.

RouchDB'nin ddoc'u değiştirip RAM'de eski tanımı tutması bu ayrımın hiçbirini tutarlı uygulamıyor. **F04/P2 korunuyor.** Düzeltmenin mutlaka replacement yapması gerekmez; PouchDB gibi tutarlı no-op da geçerli tasarım seçeneğidir.

## F05 — Aritmetik sınırları: doğrulandı, geçersiz-input anlatımı düzeltildi

CouchDB revision parser'ı generation'ı Erlang integer olarak okur. [parse_rev](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/couch/src/couch_doc.erl#L200). Mango limit/skip doğrulaması da negatif olmayan integer denetimidir; RouchDB'nin u64 sınırını zorunlu kılmaz. [mango_opts](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/mango/src/mango_opts.erl#L223).

Yeni CouchDB deneyinde MAX-u64 local revision ve normal revision, sonraki generation'a başarıyla artırıldı. >u64 skip de işlem görevini panikletmedi. Bu değerlerin **CouchDB bakımından zorunlu olarak geçersiz** olduğu varsayımı kaldırıldı.

RouchDB u64 temsili seçebilir; temsil edemediği sonucu kontrollü reddetmeli veya daha geniş temsil kullanmalıdır. RouchDB'ye özel üretilen bookmark'ın CouchDB'de aynı byte dizisiyle geçerli olması beklenmez; bookmark opaque'tir. **F05/P2, yerelde gözlenen debug panic/500 kapsamında korunuyor; release sonucu hâlâ test edilmedi.**

## F06 — Sıfır batch: PouchDB karşılaştırmasıyla güçlendi

PouchDB 9 replikasyon kodunda batch boyutu truthy fallback ile 100'e ayarlanır; sıfır etkili boyut olarak kalmaz. [Replication kaynak kodu](https://github.com/pouchdb/pouchdb/blob/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.js#L12232).

Yeni memory→memory PouchDB deneyinde `batch_size:0`, bir belgeyi gerçekten taşıdı ve hedef count 1 oldu. Aynı parametre RouchDB'de hedefi boş bırakıp başarı raporluyordu. **F06/P2 devam ediyor.** PouchDB uyumu isteniyorsa 0→100 normalleştirmesi tercih edilir; açık validation hatası da bir RouchDB API kararı olabilir, fakat PouchDB ile birebir aynı olmaz.

## F07 — Forwarded Location: hata iddiası geri çekildi

CouchDB resmî ayarları X-Forwarded-Host'un Host'tan öncelikli olduğunu belirtir. [HTTP ayarları](https://docs.couchdb.org/en/3.5.1/config/http.html#chttpd/x_forwarded_host). Sabitlenmiş `absolute_uri` kodu forwarded host/proto'yu URL üretiminde kullanır. [CouchDB absolute_uri](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/chttpd/src/chttpd.erl#L751).

Yeni gerçek CouchDB deneyinde geçerli Host yanında gönderilen forwarded başlıkları RouchDB'deki gibi `https://untrusted.invalid/...` Location üretti. Dolayısıyla gözlem gerçektir ama **tek başına CouchDB uyumsuzluğu veya kanıtlanmış güvenlik zafiyeti değildir**.

**F07 aktif hata toplamından çıkarıldı.** RouchDB'nin cookie amaçlı `trust_proxy` sınırıyla Location davranışını aynılaştırmak opsiyonel sertleştirme önerisidir. Bu, CouchDB varsayılanından bilinçli bir ayrışma olur; deployment/proxy gereksinimiyle kararlaştırılmalıdır.

## F08 — Purge state: sorun korundu, sayaç eşitliği beklentisi kaldırıldı

DB info sözleşmesi `purge_seq` alanını opak durum belirteci olarak tanımlar; işlem sayısı olarak kullanılmamasını söyler. [DB info alanı](https://docs.couchdb.org/en/3.5.1/api/database/common.html#db).

Kaynakta clustered `_purge` cevabı `purge_seq:null` üretirken DB info shard purge sequence'lerini paketler. [Purge cevabı](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/chttpd/src/chttpd_db.erl#L720), [info paketleme](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/fabric/src/fabric_db_info.erl#L72).

Yeni CouchDB deneyinde `_purge` alanı null, DB info belirteci purge öncesi/sonrası farklıydı. Bu alanların birbirine sayısal olarak eşit olması gerekmiyor. RouchDB'deki sorun, storage purge state değiştiği halde DB info'nun hep 0 üretmesi. **F08/P3 bu daraltılmış gerekçeyle devam ediyor.**

## F09 — Boş indeks: önceki validation gerekçesi geri çekildi

CouchDB JSON indeks doğrulaması alan listesini `mango_sort` yoluna gönderir; bu validator boş listeyi kabul eder. [İndeks validator](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/mango/src/mango_idx_view.erl#L219), [boş listeyi kabul eden sort validator](https://github.com/apache/couchdb/blob/44f6a43d81b83b203a110f6036d107dc2afb5556/src/mango/src/mango_sort.erl#L57). PouchDB de boş alan listesini reddeden bir koşul uygulamıyor. [PouchDB validation](https://github.com/pouchdb/pouchdb/blob/b2882c1bd006bc2aaeb6fdc83ea632c9c923b9b1/dist/pouchdb.find.js#L5408).

Yeni iki upstream deneyinde `fields:[]` için `created` döndü. Bu nedenle **RouchDB Rust API'sinin boş indeksi kabul etmesi hata kanıtı değildir**. Önceki “ortak katmanda reddedin” önerisi geri çekildi.

Kalan gözlem ters yöndedir: RouchDB HTTP 400 döndürürken Rust API, CouchDB HTTP ve PouchDB local kabul ediyor. **F09 artık API uyum farkıdır**, yanlış veri sonucu kanıtlanmış hata olarak sayılmıyor. Uyum hedefleniyorsa HTTP sözleşmesi genişletilmeli; dar kapsam bilinçli seçilecekse fark belgelenmeli.

## Yeni deneylerin kanıtları ve sınırlar

- [CouchDB referans istekleri ve yanıtları](kanitlar/upstream-reference-probes.json); [betik](kanitlar/upstream-reference-probes.py).
- [PouchDB referans sonuçları](kanitlar/pouchdb-upstream-probes.json); [betik](kanitlar/pouchdb-upstream-probes.cjs).
- [Sabit kaynak sürümleri, URL ve hash manifesti](kanitlar/online-source-manifest.json).

CouchDB deneyleri ayrı Docker container'da, PouchDB deneyleri RAM'de yürütüldü. RouchDB'nin tam test suite'leri bu online inceleme turunda tekrar çalıştırılmadı; önceki rapordaki geçerli günlükler korundu. RouchDB uygulama kodunda değişiklik yapılmadı.
