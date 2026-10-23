import {Redis} from "ioredis"

const REDIS_HOST=process.env.REDIS_HOST || "localhost"
const REDIS_PORT=Number(process.env.REDIS_PORT) || 6379

export const redisPublish= new Redis({
    host:REDIS_HOST,
    port:REDIS_PORT
})

export const redisSubscribe= new Redis({
    host:REDIS_HOST,
    port:REDIS_PORT
})

// Without an "error" listener ioredis prints "Unhandled error event" for every
// failed reconnect attempt and, on some versions, crashes the process.
// These handlers keep the server alive when redis is not running.
function logRedisError(name: string, err: Error & { errors?: Error[] }) {
  // ioredis reports connection failures as an AggregateError with an empty
  // message, so fall back to the first underlying error.
  const reason = err.message || err.errors?.[0]?.message || String(err);
  console.log(`${name} connection error: ${reason}`);
}

redisPublish.on("error",(err)=>{
  logRedisError("Redis publish", err)
})

redisSubscribe.on("error",(err)=>{
  logRedisError("Redis subscribe", err)
})