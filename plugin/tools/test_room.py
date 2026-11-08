"""Запуск: python3 plugin/tools/test_room.py
Локальный MQTT-брокер + сценарий «ведущий и два зрителя» для проверки Room и MqttClient."""
import socket, threading, time, json, sys, types, os
# берём чистую часть прямо из плагина
_src = open(sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "wtogether.plugin")).read()
W = types.ModuleType("wtogether_pure")
exec(_src[_src.index("# ===== PURE START"):_src.index("# ===== PURE END")], W.__dict__)

# ---------- мини-брокер ----------
class Broker:
    def __init__(self):
        self.srv = socket.socket(); self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0)); self.srv.listen(); self.port = self.srv.getsockname()[1]
        self.clients = {}; self.retained = {}; self.lock = threading.Lock()
        threading.Thread(target=self.accept, daemon=True).start()
    def accept(self):
        while True:
            c, _ = self.srv.accept(); threading.Thread(target=self.serve, args=(c,), daemon=True).start()
    @staticmethod
    def match(f, t):
        fp, tp = f.split("/"), t.split("/")
        for i, x in enumerate(fp):
            if x == "#": return True
            if i >= len(tp) or (x != "+" and x != tp[i]): return False
        return len(fp) == len(tp)
    def deliver(self, topic, payload, retain_flag=False):
        pkt = W._packet(0x30 | (1 if retain_flag else 0), W._enc_str(topic) + payload)
        with self.lock: targets = [(c, s) for c, s in self.clients.items()]
        for c, st in targets:
            if any(self.match(f, topic) for f in st["subs"]):
                try: c.sendall(pkt)
                except Exception: pass
    def publish(self, topic, payload, retain):
        if retain:
            with self.lock:
                if payload: self.retained[topic] = payload
                else: self.retained.pop(topic, None)
        self.deliver(topic, payload)
    def serve(self, c):
        st = {"subs": [], "will": None, "clean": False}; buf = bytearray()
        with self.lock: self.clients[c] = st
        try:
            while True:
                d = c.recv(4096)
                if not d: break
                buf += d
                while len(buf) >= 2:
                    mult, ln, i = 1, 0, 1
                    while True:
                        b = buf[i]; ln += (b & 127) * mult; mult *= 128; i += 1
                        if not b & 128: break
                    if len(buf) < i + ln: break
                    first, body = buf[0], bytes(buf[i:i+ln]); del buf[:i+ln]
                    kind = first >> 4
                    if kind == 1:  # CONNECT
                        pos = 2 + int.from_bytes(body[:2], "big") + 1
                        flags = body[pos]; pos += 3
                        cl = int.from_bytes(body[pos:pos+2], "big"); pos += 2 + cl
                        if flags & 4:
                            tl = int.from_bytes(body[pos:pos+2], "big"); wt = body[pos+2:pos+2+tl].decode(); pos += 2 + tl
                            ml = int.from_bytes(body[pos:pos+2], "big"); wm = body[pos+2:pos+2+ml]; pos += 2 + ml
                            st["will"] = (wt, wm, bool(flags & 32))
                        c.sendall(b"\x20\x02\x00\x00")
                    elif kind == 8:  # SUBSCRIBE
                        pid = body[:2]; tl = int.from_bytes(body[2:4], "big"); f = body[4:4+tl].decode()
                        st["subs"].append(f); c.sendall(b"\x90\x03" + pid + b"\x00")
                        with self.lock: ret = [(t, p) for t, p in self.retained.items() if self.match(f, t)]
                        for t, p in ret: c.sendall(W._packet(0x31, W._enc_str(t) + p))
                    elif kind == 3:
                        tl = int.from_bytes(body[:2], "big"); t = body[2:2+tl].decode()
                        self.publish(t, body[2+tl:], bool(first & 1))
                    elif kind == 12: c.sendall(b"\xd0\x00")
                    elif kind == 14: st["clean"] = True; raise ConnectionError
        except Exception: pass
        finally:
            with self.lock: self.clients.pop(c, None)
            try: c.close()
            except Exception: pass
            if st["will"] and not st["clean"]:
                self.publish(*st["will"])

# ---------- участники ----------
class Rec:
    def __init__(self, name): self.name = name; self.log = []
    def __getattr__(self, k):
        if k.startswith("on_"):
            return lambda *a, **kw: self.log.append((k, a, kw))
        raise AttributeError(k)
    def got(self, kind): return [x for x in self.log if x[0] == kind]

br = Broker(); EP = [("127.0.0.1", br.port, False)]
W.Room.CLAIM_WAIT = 0.6
def wait(cond, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        if cond(): return True
        time.sleep(0.05)
    return False
ok = lambda c, msg: print(("✅ " if c else "❌ ") + msg) or bool(c)
results = []

code = W.new_code(0)
results.append(ok(W.normalize_code(code[:2] + " " + code[2:]) == code and W.broker_of(code) == 0 and len(code) == 4,
                  f"код {code}: 4 цифры, нормализация, брокер"))

hev = Rec("host"); host = W.Room(111, "Ведущий", hev, endpoints=EP)
host.host(code, {"src": "tg", "doc": 555, "title": "Аниме 1 серия"})
results.append(ok(wait(lambda: host.claimed), "ведущий занял свободный код"))
host.publish_state(False, 0, 1.0)

# второй ведущий пытается взять тот же код — занято
xev = Rec("x"); other = W.Room(999, "Чужой", xev, endpoints=EP); other.host(code, {"src": "tg", "doc": 1})
results.append(ok(wait(lambda: xev.got("on_code_taken")) and not other.claimed, "занятый код не перехватить"))
other.client.stop(); other.client = None

OFF = -1800
g1ev = Rec("g1"); g1 = W.Room(222, "Аня", g1ev, clock=lambda: W.now_ms() + OFF, endpoints=EP)
g1.join(code)
results.append(ok(wait(lambda: g1ev.got("on_room") and g1ev.got("on_state")), "зритель сразу получил комнату и состояние"))
results.append(ok(g1.info["doc"] == 555 and g1.host_uid() == 111, "в комнате видео и ведущий"))
results.append(ok(wait(lambda: 222 in host.members and 111 in g1.members), "участники видят друг друга"))
for _ in range(3):
    g1.ping(); time.sleep(0.15)
results.append(ok(wait(lambda: g1.rtt is not None) and abs(g1.offset - 1800) < 60, f"сверка часов: {g1.offset} мс при 1800"))

g1.set_progress("ready", 100)
results.append(ok(wait(lambda: host.members.get(222, {}).get("s") == "ready"), "ведущий видит «готов»"))
host.publish_state(True, 0, 1.0, ts=W.now_ms() + 1500)
results.append(ok(wait(lambda: g1.st and g1.st["p"]), "старт с отсчётом дошёл"))
results.append(ok(W.plan_correction(g1.st, g1.room_now(), 0, False)[0] is False, "во время отсчёта зритель на паузе"))
time.sleep(1.7)
results.append(ok(W.plan_correction(g1.st, g1.room_now(), 0, False)[0] is True, "после отсчёта зритель играет"))

host.chat("Смотри!", 1000); g1.chat("Ого", 1200); g1.react("🔥")
results.append(ok(wait(lambda: g1ev.got("on_chat") and hev.got("on_chat") and hev.got("on_react")), "чат и реакции"))
results.append(ok(len(hev.got("on_chat")) == 1 and not g1ev.got("on_react"), "свои сообщения и реакции не возвращаются"))
g1.ask_wait()
results.append(ok(wait(lambda: hev.got("on_wait")) and hev.got("on_wait")[0][1][0]["n"] == "Аня", "«подожди меня» дошло до ведущего"))

g2ev = Rec("g2"); g2 = W.Room(333, "Боря", g2ev, endpoints=EP); g2.join(code)
host.publish_state(True, 65000, 1.5)
results.append(ok(wait(lambda: g2.st and g2.st["pos"] == 65000 and g2.st["r"] == 1.5), "опоздавший получил позицию и скорость"))
results.append(ok(wait(lambda: 333 in host.members and 333 in g1.members), "все видят нового участника"))

# передача управления Ане
host.transfer(222)
results.append(ok(wait(lambda: g1.role == "host" and host.role == "guest"), "👑 передана: Аня ведущая, бывший ведущий — зритель"))
results.append(ok(wait(lambda: g2ev.got("on_host_changed") and g2.host_uid() == 222), "остальные узнали о новом ведущем"))
g1.publish_state(False, 70000, 1.0)
results.append(ok(wait(lambda: host.st and host.st["pos"] == 70000 and g2.st["pos"] == 70000), "новый ведущий управляет всеми"))
host.ping(); g2.ping()
results.append(ok(wait(lambda: host.rtt is not None and g2.rtt is not None), "часы сверяются с новым ведущим"))
results.append(ok(wait(lambda: g2.members.get(222, {}).get("host") and not g2.members.get(111, {}).get("host")),
                  "корона в списке участников переехала"))

g2.client.running = False; g2.client.sock.close()
results.append(ok(wait(lambda: 333 not in g1.members, 6), "обрыв связи — участник исчез из списка"))

# новый ведущий (Аня) заканчивает
g1.close()
results.append(ok(wait(lambda: hev.got("on_end")), "все узнали, что просмотр закончен"))
host.close()

# брошенная комната: ведущий пропал без «закончить» — код можно занять снова
code2 = W.new_code(0)
aev = Rec("a"); a = W.Room(501, "Пропал", aev, endpoints=EP); a.host(code2, {"src": "tg", "doc": 7})
wait(lambda: a.claimed); a.client.running = False; a.client.sock.close(); time.sleep(0.5)
bev = Rec("b"); b = W.Room(502, "Новый", bev, endpoints=EP); b.host(code2, {"src": "tg", "doc": 8})
results.append(ok(wait(lambda: b.claimed) and not bev.got("on_code_taken"), "код брошенной комнаты снова свободен"))

print(f"\nИтого: {sum(results)} из {len(results)}")
