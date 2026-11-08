# Симуляция синхронизации: ведущий и зритель, задержка сообщений, сдвиг часов,
# зритель с «плохим» плеером. Логика зрителя — та же plan_correction из плагина.
import random, re, sys
src = open(sys.argv[1]).read()
pure = src.split("# ---------------- обёртки Java-интерфейсов")[0]
pure = pure[pure.index("__id__"):]
ns = {}
exec("import json, re, time\n" + pure, ns)
plan, SEEK_COOLDOWN = ns["plan_correction"], ns["SEEK_COOLDOWN"]

class P:  # модель плеера
    def __init__(s, jitter=0.0): s.pos=0.0; s.play=False; s.speed=1.0; s.jit=jitter
    def step(s, dt):
        if s.play: s.pos += dt*s.speed*(1+random.uniform(-s.jit, s.jit))

random.seed(1)
DT=10; OFF=-1800        # часы зрителя отстают на 1.8 с
lat=lambda: random.randint(250, 1200)
host, guest = P(), P(jitter=0.03)
inbox=[]; remote=None; go=None; nud=False; offset_est=0; block=0; t=0; worst=[]
def send(at, m): inbox.append((at+lat(), m))
# пинг-понг для оценки часов
for k in range(3):
    a = t+OFF+k*1000; rtt_out=lat(); rtt_back=lat()
    b = t+k*1000+rtt_out
    est = b + (rtt_out+rtt_back)//2 - (a+rtt_out+rtt_back)
    if k==0 or rtt_out+rtt_back < best: best=rtt_out+rtt_back; offset_est=est
print(f"реальный сдвиг {-OFF} мс, оценка {offset_est} мс")
events={2000:"go", 40000:"pause", 46000:"play", 60000:"seek", 80000:"speed", 100000:"guest_glitch"}
beat=0; started=False; countdown=None
while t < 130000:
    if t in events:
        e=events[t]
        if e=="go": countdown=t+3000; send(t,{"t":"go","pos":0,"at":t+3000,"r":1.0})
        if e=="pause": host.play=False; send(t,{"t":"st","p":False,"pos":host.pos,"r":host.speed,"ts":t})
        if e=="play": host.play=True; send(t,{"t":"st","p":True,"pos":host.pos,"r":host.speed,"ts":t})
        if e=="seek": host.pos=95000; send(t,{"t":"st","p":True,"pos":host.pos,"r":host.speed,"ts":t})
        if e=="speed": host.speed=1.5; send(t,{"t":"st","p":True,"pos":host.pos,"r":1.5,"ts":t})
        if e=="guest_glitch": guest.pos+=7000
    if countdown and t>=countdown:
        host.play=True; countdown=None; started=True; send(t,{"t":"st","p":True,"pos":host.pos,"r":1.0,"ts":t}); beat=t
    if host.play and t-beat>=3000: beat=t; send(t,{"t":"st","p":True,"pos":host.pos,"r":host.speed,"ts":t})
    for item in [x for x in inbox if x[0]<=t]:
        inbox.remove(item); m=item[1]
        if m["t"]=="go": go=m; remote=None
        else: go=None; remote=m
    gnow = t+OFF+offset_est  # часы ведущего глазами зрителя
    if t%300==0:
        if go:
            if gnow>=go["at"]: remote={"p":True,"pos":go["pos"],"r":go["r"],"ts":go["at"]}; go=None
        if remote:
            play, seek, sp, nud = plan(remote, gnow, guest.pos, guest.play, nud)
            if seek is not None and t/1000>block: guest.pos=seek; block=t/1000+SEEK_COOLDOWN
            guest.play=play; guest.speed=sp if play else guest.speed
    host.step(DT); guest.step(DT); t+=DT
    if t%5000==0:
        tg = ns["expected_pos"](remote, gnow) if remote else 0
        worst.append((t, round(guest.pos-host.pos), round(guest.pos-tg), round(host.pos-tg), nud, round(guest.speed,2)))
for tt,d,dg,dh,n,sp in worst: print(f"{tt/1000:5.0f} с  итог {d:+6d}  зритель-цель {dg:+6d}  ведущий-цель {dh:+6d}  {n} {sp}")
