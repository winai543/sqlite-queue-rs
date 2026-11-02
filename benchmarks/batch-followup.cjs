// Follow-up with 20 paired runs, alternating binary order.
const {SQLiteQueue}=require('../lib/index.cjs');
const {mkdtempSync,rmSync,writeFileSync}=require('node:fs');
const {tmpdir}=require('node:os');
const {performance}=require('node:perf_hooks');
const path=require('node:path');
const root=mkdtempSync(path.join(tmpdir(),'queue-batch-check-'));
const {resolveExtension}=require('../lib/index.cjs');
if(!process.env.QUEUE_BENCH_BEFORE)throw new Error('Set QUEUE_BENCH_BEFORE to the preserved original extension path.');
const binaries={before:process.env.QUEUE_BENCH_BEFORE,after:resolveExtension()};
const payload={type:'email',to:'alice@example.com',body:'x'.repeat(200)};
const {createHash}=require('node:crypto');
const {readFileSync}=require('node:fs');
const {cpus}=require('node:os');
const hashes=Object.fromEntries(Object.entries(binaries).map(([label,file])=>[label,createHash('sha256').update(readFileSync(file)).digest('hex')]));
let environment;
const results=[];
try{
 for(let round=0;round<20;round++)for(const label of round%2?['after','before']:['before','after']){
  const s=new SQLiteQueue(path.join(root,`${round}-${label}.db`),{extensionPath:binaries[label]});
  try{
   s.database.exec('PRAGMA synchronous=NORMAL');
   if(!environment){const pragma=name=>Object.values(s.database.prepare(`PRAGMA ${name}`).get())[0];environment={cpu:cpus()[0].model,node:process.version,sqlite:s.database.prepare('SELECT sqlite_version() AS v').get().v,platform:process.platform,arch:process.arch,payloadBytes:Buffer.byteLength(JSON.stringify(payload)),journalMode:pragma('journal_mode'),pageSize:pragma('page_size'),walAutocheckpoint:pragma('wal_autocheckpoint'),fullfsync:pragma('fullfsync'),background:false,clients:1};}
   const q=s.queue('bench');q.push(payload);const warm=q.claim();q.ack(warm.id,warm.receipt);q.purge({olderThanSeconds:0});
   s.transaction(()=>q.pushBatch(Array.from({length:1000},()=>({data:payload}))));
   const start=performance.now();
   for(let i=0;i<10;i++){const jobs=q.claimBatch(100,{leaseSeconds:300});if(jobs.length!==100)throw Error('missing job');s.transaction(()=>{for(const j of jobs)if(!q.ack(j.id,j.receipt))throw Error('ack failed');});}
   const elapsedMs=performance.now()-start;
   if(q.stats().done!==1000)throw Error('missing acknowledgements');
   results.push({round:round+1,label,elapsedMs,jobsPerSec:1000/elapsedMs*1000});
  }finally{s.close();}
 }
 const median=a=>a.sort((a,b)=>a-b)[Math.floor(a.length/2)];
 const summary={before:median(results.filter(r=>r.label==='before').map(r=>r.jobsPerSec)),after:median(results.filter(r=>r.label==='after').map(r=>r.jobsPerSec))};
 writeFileSync(path.join(__dirname,'results','batch-followup.json'),JSON.stringify({scenario:'claim_batch_100_ack_transaction',mode:'NORMAL',jobsPerRun:1000,pairedRounds:20,order:'alternating before/after',summary,results,environment,beforeExtensionSha256:hashes.before,afterExtensionSha256:hashes.after},null,2)+'\n');
 console.log(JSON.stringify(summary));
}finally{rmSync(root,{recursive:true,force:true});}
