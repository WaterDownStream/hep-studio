# Representative source excerpts

These excerpts are copied from the supplied source snapshot. Full files, dependencies and tests are in the source ZIP.

## runtime/project_store.cpp

```cpp
#include "runtime/project_store.hpp"
#include <QSaveFile>
#include <QFile>
#include <QLockFile>
#include <QCryptographicHash>
#include <stdexcept>
namespace hep {
void saveProject(const Project& project,const QString& path) {
    QLockFile lock(path+".lock");
    if(!lock.tryLock(0)) throw std::runtime_error("Project is being written by another process");
    const auto text=serialize(project);
    if(text.size()>16*1024*1024) throw std::runtime_error("Project exceeds the Phase 0 16 MiB limit; archive run history before saving");
    QSaveFile file(path); file.setDirectWriteFallback(false);
    if(!file.open(QIODevice::WriteOnly)) throw std::runtime_error(file.errorString().toStdString());
    if(file.write(text.data(),static_cast<qint64>(text.size()))!=static_cast<qint64>(text.size()) || !file.commit())
        throw std::runtime_error(file.errorString().toStdString());
}
Project loadProject(const QString& path) {
    QFile file(path);
    if(!file.open(QIODevice::ReadOnly)) throw std::runtime_error(file.errorString().toStdString());
    if(file.size()>16*1024*1024) throw std::runtime_error("Project exceeds the Phase 0 16 MiB limit");
    const auto bytes=file.readAll();
    return parseProject(std::string_view(bytes.constData(),static_cast<std::size_t>(bytes.size())));
}
QString workflowHash(const Json& snapshot) {
    const auto text=snapshot.dump();
    return QString::fromLatin1(QCryptographicHash::hash(QByteArray(text.data(),static_cast<qsizetype>(text.size())),QCryptographicHash::Sha256).toHex());
}
}
```

## plugins/collider/parallel.py

```python
"""Deterministic process partitions over the existing version-1 worker protocol.

Only compact accumulators return to the parent. Child stdin EOF terminates native
work if the parent dies; every normal/error/cancel path also reaps its children.
"""
import concurrent.futures,copy,json,math,os,queue,struct,subprocess,sys,threading,time
from pathlib import Path

MAX_FRAME=1024*1024
MAX_RESULT=16*1024*1024

def partitions(seed,events,workers):
    if type(workers) is not int or not 1<=workers<=32:raise ValueError('Generator workers must be an integer in [1,32]')
    if type(events) is not int or events<workers:raise ValueError('Event count must be at least the worker count')
    # A versioned injective seed assignment within a job; no random collisions.
    return [{'index':i,'seed':1+(seed-1+i)%900000000,'events':events//workers+(i<events%workers),'firstEvent':i*(events//workers)+min(i,events%workers)} for i in range(workers)]

def logical_partitions(seed,events,config):
    policy=config.get('streamPolicy','legacy-streams-v1')
    if policy=='legacy-streams-v1':
        result=partitions(seed,events,config.get('logicalStreams',config.get('workers',1)))
        offset=0
        for part in result:part['firstEvent']=offset;offset+=part['events']
        return result
    if policy!='logical-blocks-v2':raise ValueError('Unknown random stream policy')
    block=config.get('partitionEvents',4096)
    if type(block) is not int or not 1<=block<=1000000:raise ValueError('Invalid logical partition size')
    if type(events) is not int or not 1<=events<=1000000000:raise ValueError('Invalid event target')
    count=(events+block-1)//block
    if count>4096:raise ValueError('More than 4096 logical blocks; choose a larger partitionEvents value')
    return [{'index':i,'seed':1+(seed-1+i)%900000000,'events':min(block,events-i*block),'firstEvent':i*block} for i in range(count)]

def _send(process,message):
    data=json.dumps(dict(protocolVersion=1,**message),allow_nan=False,separators=(',',':')).encode()
    if len(data)>MAX_FRAME:raise ValueError('Partition request exceeds control-frame limit')
    packet=memoryview(struct.pack('!I',len(data))+data)
    while packet:
        written=process.stdin.write(packet)
        if not written:raise BrokenPipeError('Partition input closed')
        packet=packet[written:]
    process.stdin.flush()

def _reader(stream,messages):
    def exact(size):
        value=b''
        while len(value)<size:
            block=stream.read(size-len(value))
            if not block:raise EOFError('Collider partition exited or sent an incomplete frame')
            value+=block
        return value
    try:
        while True:
            size=struct.unpack('!I',exact(4))[0]
            if not 0<size<=MAX_FRAME:raise ValueError('Invalid partition frame length')
            message=json.loads(exact(size));message['_frameBytes']=size+4
            if message.get('protocolVersion')!=1:raise ValueError('Partition protocol mismatch')
            messages.put(message)
    except Exception as error:messages.put(error)

def run_partition(payload,part,cancel,progress,backend_path,engine_path):
    worker=backend_path/'worker.py'
    if not worker.exists():worker=backend_path.parent/'root'/'worker.py'
    if not worker.is_file():raise ValueError('Collider worker script unavailable')
    bootstrap='import sys,runpy;sys.path.insert(0,sys.argv[1]);runpy.run_path(sys.argv[2],run_name="__main__")'
    env=dict(os.environ,HEP_COLLIDER_ENGINE=str(engine_path),OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1')
    process=subprocess.Popen([sys.executable,'-u','-c',bootstrap,str(backend_path),str(worker)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,bufsize=0)
    messages=queue.Queue();logs=bytearray()
    def diagnostics():
        while True:
            block=process.stderr.read(4096)
            if not block:return
            logs.extend(block)
            if len(logs)>65536:del logs[:-65536]
    readers=[threading.Thread(target=_reader,args=(process.stdout,messages),daemon=True),threading.Thread(target=diagnostics,daemon=True)]
    for reader in readers:reader.start()
    chunks=[];size=0;wire_bytes=0;ready=False;last=time.monotonic();began=last
    try:
        while True:
            if cancel.is_set():raise InterruptedError('Collider job cancelled')
            try:message=messages.get(timeout=.1)
            except queue.Empty:
                if time.monotonic()-last>30 or not ready and time.monotonic()-began>30:raise TimeoutError('Collider partition handshake/heartbeat timed out')
                continue
            if isinstance(message,Exception):raise message
            last=time.monotonic();wire_bytes+=message.get('_frameBytes',0);kind=message.get('kind')
            if kind=='hello':
                if ready or message.get('capability',{}).get('backend')!='hep.collider/1' or not message.get('capability',{}).get('available'):raise ValueError('Collider partition unavailable/incompatible')
                if message['capability'].get('environment',{}).get('id')!=payload['_partitionEnvironmentId']:raise ValueError('Partition environment differs from the frozen parent environment; restart the runtime')
                child=copy.deepcopy(payload)
                for node in child['workflow']['nodes']:
                    if node['typeId']=='hep.collider.Pythia':node['config'].update(events=part['events'],workers=1,seedStrategy='fixed',seed=str(part['seed']))
                child['eventOffset']=part.get('firstEvent',0);child['allocatedThreads']=1;child['inspectEvents']=1 if part['index']==0 else 0
                progress(part['index'],{'pid':process.pid,'stage':'Partition ready'})
                _send(process,{'kind':'request','id':'partition','operation':'executePartition','payload':child,'resultTransport':'chunks'});ready=True
            elif kind=='heartbeat':pass
            elif kind=='progress':progress(part['index'],message.get('metrics',{}))
            elif kind=='resultChunk':
                if message.get('id')!='partition' or message.get('sequence')!=len(chunks):raise ValueError('Invalid partition result sequence')
                chunk=message['data'];size+=len(chunk.encode())
                if size>MAX_RESULT:raise ValueError('Partition result exceeds limit')
```

## plugins/research/statistical.py

```python
"""Structured RooFit/RooStats lowering, explicit statistical assumptions."""
from __future__ import annotations
import array,math,uuid
from artifacts import key,check_cancel
FIT_HELPER=False

def fit_native(pdf,data,options,ROOT):
    global FIT_HELPER
    if not FIT_HELPER:
        if not ROOT.gInterpreter.Declare('''
#include <RooAbsPdf.h>
#include <RooFitResult.h>
#include <RooLinkedList.h>
#include <memory>
namespace hep_research_stats {
RooFitResult* own(RooFitResult* p){return p;}
RooFitResult* own(std::unique_ptr<RooFitResult> p){return p.release();}
RooFitResult* fit(RooAbsPdf& p,RooAbsData& d,const RooLinkedList& c){return own(p.fitTo(d,c));}
}
'''):raise RuntimeError('RooFit worker helper compilation failed')
        ROOT.hep_research_stats.fit.__release_gil__=True;FIT_HELPER=True
    commands=ROOT.RooLinkedList()
    for option in options:commands.Add(option)
    result=ROOT.hep_research_stats.fit(pdf,data,commands)
    if result:ROOT.SetOwnership(result,True)
    return result

def histogram(histogram,ROOT):
    edges=histogram['edges'];values=histogram['values'];errors=histogram['errors']
    if len(edges)!=len(values)+1 or len(errors)!=len(values) or not values:raise ValueError('Incompatible histogram template shape')
    if any(not math.isfinite(float(v)) for v in edges+values+errors) or any(v<0 for v in values+errors):raise ValueError('Statistical templates require finite nonnegative contents/errors')
    if any(a>=b for a,b in zip(edges,edges[1:])) or sum(values)<=0:raise ValueError('Statistical template has empty integral or invalid bins')
    h=ROOT.TH1D('stat_'+uuid.uuid4().hex,'',len(values),array.array('d',edges));h.SetDirectory(0);h.Sumw2()
    for i,(v,e) in enumerate(zip(values,errors),1):h.SetBinContent(i,v);h.SetBinError(i,e)
    return h

def fit(hist,config,root,cancel=None):
    check_cancel(cancel);ROOT=root.ROOT;h=histogram(hist,ROOT);owners=[];parameters={};edges=hist['edges']
    bounds=config.get('range',[edges[0],edges[-1]])
    if len(bounds)!=2 or not edges[0]<=bounds[0]<bounds[1]<=edges[-1]:raise ValueError('Fit range must lie within the identified histogram')
    if any(not any(math.isclose(float(bound),float(edge),rel_tol=1e-10,abs_tol=1e-12) for edge in edges) for bound in bounds):raise ValueError('A binned fit range must coincide with histogram bin edges')
    x=ROOT.RooRealVar('x','Observable',float(edges[0]),float(edges[-1]));owners.append(x)
    def parameter(spec,role):
        name=spec.get('name',role)
        if name in parameters:raise ValueError('Duplicate RooFit parameter identity: '+name)
        value=float(spec['value']);lo=float(spec['lower']);hi=float(spec['upper'])
        if not all(math.isfinite(v) for v in (value,lo,hi)) or not lo<hi or not lo<=value<=hi:raise ValueError('Invalid RooFit initial parameter/bounds')
        p=ROOT.RooRealVar(name,name,value,lo,hi);p.setConstant(bool(spec.get('constant',False)));owners.append(p);parameters[name]=p;return p
    def model(spec,depth=0):
        if depth>12:raise ValueError('Statistical model nesting limit')
        name='model_'+uuid.uuid4().hex;kind=spec['kind']
        if kind=='Gaussian':
            mean=parameter(spec['mean'],'mean');sigma=parameter(spec['sigma'],'sigma')
            if sigma.getMin()<=0:raise ValueError('Gaussian width must remain positive')
            result=ROOT.RooGaussian(name,name,x,mean,sigma)
        elif kind=='BreitWigner':
            mean=parameter(spec['mean'],'mean');width=parameter(spec['width'],'width')
            if width.getMin()<=0:raise ValueError('Breit-Wigner width must remain positive')
            result=ROOT.RooBreitWigner(name,name,x,mean,width)
        elif kind=='Exponential':result=ROOT.RooExponential(name,name,x,parameter(spec['slope'],'slope'))
        elif kind=='Polynomial':
            coefficients=spec['coefficients']
            if not 1<=len(coefficients)<=5:raise ValueError('Polynomial order must be from one to five')
            args=ROOT.RooArgList()
            for i,c in enumerate(coefficients):args.add(parameter(c,'c'+str(i+1)))
            result=ROOT.RooPolynomial(name,name,x,args)
        elif kind=='Histogram':
            template=spec['template']
            if template['edges']!=edges:raise ValueError('Histogram PDF binning differs from its observable dataset')
            th=histogram(template,ROOT);data=ROOT.RooDataHist(name+'_data',name+'_data',ROOT.RooArgList(x),th);owners.extend([th,data])
            result=ROOT.RooHistPdf(name,name,ROOT.RooArgSet(x),data,0)
        elif kind in ('Sum','Product'):
            children=spec['components']
            if not 2<=len(children)<=8:raise ValueError('Model composition requires two to eight components')
            pdfs=ROOT.RooArgList()
            for child in children:pdfs.add(model(child,depth+1))
            if kind=='Product':result=ROOT.RooProdPdf(name,name,ROOT.RooArgSet(pdfs))
            else:
                if len(spec['fractions'])!=len(children)-1:raise ValueError('Mixture requires one fewer fractions than components')
                fractions=ROOT.RooArgList()
                for i,f in enumerate(spec['fractions']):
                    if f['lower']<0 or f['upper']>1:raise ValueError('Mixture fractions must lie between zero and one')
                    fractions.add(parameter(f,'fraction'+str(i)))
                # Recursive fractions ensure positive, normalized mixtures.
                result=ROOT.RooAddPdf(name,name,pdfs,fractions,True)
        else:raise ValueError('Unsupported structured RooFit model: '+kind)
        owners.append(result);return result
    pdf=model(config['model']);constraints=ROOT.RooArgSet()
    if not any(not p.isConstant() for p in parameters.values()):raise ValueError('This fit has no floating parameters; use a fixed PDF as a component of a model or compare its predictions without claiming an optimizer fit')
    for spec in config.get('constraints',[]):
        if spec.get('kind')!='Gaussian' or spec['parameter'] not in parameters:raise ValueError('Unsupported explicit fit constraint')
        sigma=float(spec['sigma']);mean=float(spec['mean'])
        if not math.isfinite(mean+sigma) or sigma<=0:raise ValueError('Constraint requires finite mean and positive width')
        center=ROOT.RooConstVar('constraint_mean_'+str(len(owners)),'',mean);width=ROOT.RooConstVar('constraint_sigma_'+str(len(owners)),'',sigma)
        constraint=ROOT.RooGaussian('constraint_'+str(len(owners)),'',parameters[spec['parameter']],center,width);owners.extend([center,width,constraint]);constraints.add(constraint)
    mode=config.get('errorModel')
    if mode not in ('poisson','sumw2'):raise ValueError('Choose Poisson counts or explicitly weighted SumW2 fit errors')
    if mode=='poisson' and (hist.get('normalization','counts')!='counts' or any(abs(v-round(v))>1e-8 or not math.isclose(e*e,v,rel_tol=1e-6,abs_tol=1e-8) for v,e in zip(hist['values'],hist['errors']))):raise ValueError('Poisson fit requires identified unscaled integer counts with counting variances')
    data=ROOT.RooDataHist('fit_data','',ROOT.RooArgList(x),h);owners.append(data);x.setRange('fit',float(bounds[0]),float(bounds[1]))
    options=[ROOT.RooFit.Save(True),ROOT.RooFit.PrintLevel(-1),ROOT.RooFit.Range('fit'),ROOT.RooFit.IntegrateBins(1e-5),ROOT.RooFit.SumW2Error(mode=='sumw2')]
```
