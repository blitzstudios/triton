use rustler::error::Error;
use rustler::{Binary, NifResult};
use scylla::routing::partitioner::Partitioner;
use scylla::routing::partitioner::PartitionerHasher;
use scylla::routing::partitioner::PartitionerName;
use scylla::routing::Sharder;
use scylla::routing::Token;
use std::num::NonZero;
use std::env;

static ERROR_VALUE_TOO_LONG: &str = "value too long";

// this is taken from the scylla rust driver code with very slight modifications (around data serialization)
fn compute_partition_token(data: Vec<Binary>) -> Result<Token, &'static str> {
    let partitioner_hasher = &mut PartitionerName::Murmur3.build_hasher();

    if data.len() == 1 {
        let val = &data[0];
        partitioner_hasher.write(val.as_slice());
    } else {
        for val in data {
            let val_bytes = val.as_slice();
            let val_len_u16: u16 = val_bytes
                .len()
                .try_into()
                .map_err(|_| ERROR_VALUE_TOO_LONG)?;
            partitioner_hasher.write(&val_len_u16.to_be_bytes());
            partitioner_hasher.write(val_bytes);
            partitioner_hasher.write(&[0u8]);
        }
    }

    Ok(partitioner_hasher.finish())
}

#[rustler::nif]
fn shard_from_partition_key(data: Vec<Binary>) -> NifResult<u32> {
    let num_shards = match env::var("SCYLLA_NUM_SHARDS") {
        Ok(val) => match val.parse::<u16>() {
            Ok(num_shards) => num_shards,
            Err(_) => 32,
        },
        Err(_) => 32,
    };
    let sharder = Sharder::new(NonZero::new(num_shards).unwrap(), 12);
    let partition_token = compute_partition_token(data).map_err(|e| Error::Atom(e))?;
    let shard = sharder.shard_of(partition_token);
    Ok(shard as u32)
}

rustler::init!("Elixir.Triton.APM.ShardInfo");
