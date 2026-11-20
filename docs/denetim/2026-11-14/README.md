# RouchDB kaynak kodu denetimi — 14 Kasım 2026

İncelenen kaynak: `d1259449bb1ad20d2df2626989b3a581b7e7893f`, workspace 0.5.1.

- [Teknik doküman](TEKNIK-DOKUMAN.md): mimari, veri modeli, depolama, yazma, replikasyon, API, güvenlik ve işletim.
- [Denetim raporu](DENETIM-RAPORU.md): 7 davranış hatası, 1 API uyum farkı, 1 sertleştirme önerisi ve 6 eksik/risk alanı, kanıtlar, düzeltme önerileri ve kabul ölçütleri.
- [Dokuz maddenin online kaynak doğrulaması](ONLINE-KAYNAK-DOGRULAMASI.md): sürüme sabit CouchDB/PouchDB kaynakları ve yeniden değerlendirme.
- [Kanıtları yeniden çalıştırma](kanitlar/README.md): test günlükleri, source manifesti, GitNexus sonuçları, HTTP ve Rust deneyleri.

Öncelikli sorunlar: PUT tombstone metadata kaybı; arbitrary-precision sayılarda yanlış eşitlik; ortak adapter reset'i sonrasında bayat Mango indeksi.

Mevcut test sonuçları: normal ve arbitrary-precision suite'lerinin her birinde 1006 başarılı test; ayrıca 130 CouchDB testi, büyük sayı CouchDB testi ve PouchDB T1–T9 geçti. Bu başarılar rapordaki kenar durumlarını kapsamıyor. Uygulama kaynakları değiştirilmedi; düzeltmeler uygulanmadı.
