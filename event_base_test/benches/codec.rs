use criterion::{Criterion, black_box, criterion_group, criterion_main};
use event_base_core::message::{DeliveryMode, EMessage, MessagePayload, MessageTopic};
use event_base_core::wal::codec::{BincodeCodec, WalRecordCodec};
use event_base_core::wal::wal::WalRecord;
use std::time::{Duration, SystemTime};

fn sample_record() -> WalRecord {
    let mut message = EMessage::new(
        MessageTopic("bench.topic".to_string()),
        MessagePayload(vec![1; 256]),
        DeliveryMode::Repeated(3),
        Some("worker-a".to_string()),
    );
    message.deliver_at = Some(SystemTime::now() + Duration::from_secs(30));
    let mut record = WalRecord::from_msg(message);
    record.record_id = 1;
    record
}

fn codec_benchmark(c: &mut Criterion) {
    let codec = BincodeCodec;
    let record = sample_record();

    c.bench_function("wal_record_encode", |bench| {
        bench.iter(|| {
            let bytes = codec
                .encode(black_box(&record))
                .expect("encode should succeed");
            black_box(bytes);
        });
    });

    let encoded = codec.encode(&record).expect("seed encode should succeed");
    c.bench_function("wal_record_decode", |bench| {
        bench.iter(|| {
            let decoded = codec
                .decode(black_box(&encoded))
                .expect("decode should succeed");
            black_box(decoded);
        });
    });
}

criterion_group!(benches, codec_benchmark);
criterion_main!(benches);
