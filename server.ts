import WebSocket,{WebSocketServer} from "ws";
import express from "express";
import http from "http";
import path from "path";
import {fileURLToPath} from "url";
import {redisPublish,redisSubscribe} from "./connection.ts"


const PORT=process.env.PORT || 9000;
const REDIS_CHANNEL='ws-message'

const app = express();
const server=http.createServer(app);
const wss=new WebSocketServer({server})

// Serve the frontend (public/server.html) at http://localhost:PORT
const __dirname = path.dirname(fileURLToPath(import.meta.url));
app.use(express.static(path.join(__dirname, "public")));
app.get("/",(req,res)=>{
  res.sendFile(path.join(__dirname, "public", "server.html"))
})

redisSubscribe.subscribe(REDIS_CHANNEL).catch((err)=>{
  console.log("Failed to subscribe to redis:", err.message)
})
redisSubscribe.on("message",(channel,message)=>{
  if(channel==REDIS_CHANNEL){
    wss.clients.forEach((c)=>{
      // only send to sockets that are still open, sending to a closed one throws
      if(c.readyState===WebSocket.OPEN){
        c.send(message.toString())
      }
    })
  }
})


wss.on("connection",(ws:WebSocket)=>{
  console.log("Client connected");

  ws.on("error",(err)=>{
    console.log("WebSocket connection error:", err.message)
  })
  ws.on("message", async (data) => {
  const message = data.toString();
  console.log(`Received message: ${message}`);
  // relay the message to the redis the parent server
  console.log("server is sending to the redis")
  try{
    await redisPublish.publish(REDIS_CHANNEL,message)
  }catch(err){
    console.log("Failed to publish to redis:", (err as Error).message)
  }
  });   
})


server.on("error",(err)=>{
  console.log("Server error:", err.message)
  process.exit(1)
})

wss.on("error",(err)=>{
  console.log("WebSocket server error:", err.message)
})

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
