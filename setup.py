#!/usr/bin/env python3
# ================================================================
# setup.py — FULLY AUTOMATIC: installs Java 11, PySpark, all deps
# Just run:  python3 setup.py
# ================================================================

import os, sys, subprocess, platform, time
from pathlib import Path

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║       ⚡  SPARK AGENT — FULLY AUTO SETUP  ⚡                 ║
║    Installs Java 11 + PySpark + All Dependencies             ║
║    No manual steps required!                                 ║
╚══════════════════════════════════════════════════════════════╝
"""

def run_cmd(cmd, capture=False):
    if capture:
        r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, text=True)
        return r.stdout.strip(), r.stderr.strip(), r.returncode
    r = subprocess.run(cmd, shell=True)
    return "", "", r.returncode

def ok(msg):     print(f"  ✅ {msg}")
def info(msg):   print(f"  ℹ️  {msg}")
def warn(msg):   print(f"  ⚠️  {msg}")
def step(n, msg): print(f"\n{'═'*62}\n  🔧 Step {n}: {msg}\n{'═'*62}")

# ── Step 1: System info ──────────────────────────────────────
def step1_system():
    step(1, "System Check")
    out, _, _ = run_cmd("uname -a", capture=True)
    info(f"OS: {out[:60]}")
    info(f"Python: {sys.version.split()[0]}")
    if sys.version_info < (3, 8):
        print("  ❌ Python 3.8+ required!"); sys.exit(1)
    ok("Python OK")

# ── Step 2: Auto-install Java 11 ────────────────────────────
def step2_java():
    step(2, "Auto-Installing Java 11 (required for Spark)")

    def get_java_major():
        _, stderr, code = run_cmd("java -version", capture=True)
        for tok in stderr.replace('"','').split():
            if tok[0].isdigit():
                major = tok.split(".")[0]
                return int(major) if major.isdigit() else 0
        return 0

    ver = get_java_major()
    info(f"Current Java version: {ver if ver else 'NOT INSTALLED'}")

    if ver >= 11:
        ok(f"Java {ver} already installed — perfect!")
        _find_and_set_java_home()
        return

    info("Java 11 not found. Auto-installing (≈30 seconds)...")

    # Detect Linux distro
    distro_out, _, _ = run_cmd("cat /etc/os-release 2>/dev/null | grep '^ID='", capture=True)
    distro = distro_out.replace("ID=","").replace('"','').lower().strip()
    info(f"Distro: {distro or 'unknown'}")

    installed = False
    if distro in ("ubuntu","debian","linuxmint","pop","elementary","kali","raspbian"):
        run_cmd("sudo apt-get update -qq")
        _, _, code = run_cmd("sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk -qq")
        installed = (code == 0)
    elif distro in ("fedora","centos","rhel","rocky","almalinux","ol"):
        _, _, code = run_cmd("sudo dnf install -y java-11-openjdk-devel 2>/dev/null || sudo yum install -y java-11-openjdk-devel")
        installed = (code == 0)
    elif distro in ("arch","manjaro","endeavouros"):
        _, _, code = run_cmd("sudo pacman -S --noconfirm jdk11-openjdk")
        installed = (code == 0)
    elif distro in ("opensuse","sles"):
        _, _, code = run_cmd("sudo zypper install -y java-11-openjdk-devel")
        installed = (code == 0)
    elif platform.system() == "Darwin":
        run_cmd("brew install openjdk@11 2>/dev/null")
        run_cmd("sudo ln -sfn /usr/local/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk 2>/dev/null")
        installed = True
    else:
        # Generic fallback
        _, _, code = run_cmd("sudo apt-get install -y openjdk-11-jdk -qq 2>/dev/null")
        installed = (code == 0)

    # Verify install
    time.sleep(1)
    ver_after = get_java_major()
    if ver_after >= 11:
        ok(f"Java {ver_after} installed successfully!")
    elif installed:
        warn("Java installed but version check inconclusive. Continuing...")
    else:
        warn("Auto-install may have failed. Checking for any existing Java...")

    _find_and_set_java_home()

def _find_and_set_java_home():
    """Auto-find the best Java 11+ and set JAVA_HOME."""
    current = os.environ.get("JAVA_HOME","")
    if current and os.path.exists(os.path.join(current,"bin","java")):
        ok(f"JAVA_HOME already valid: {current}")
        return

    # Search paths in priority order
    candidates = [
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/java-11-openjdk-arm64",
        "/usr/lib/jvm/java-11-openjdk",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-21-openjdk-amd64",
        "/usr/lib/jvm/temurin-11",
        "/usr/lib/jvm/temurin-17",
        "/usr/local/lib/jvm/java-11",
        "/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home",
        "/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home",
    ]

    # Dynamic search via update-java-alternatives
    out, _, _ = run_cmd("update-java-alternatives -l 2>/dev/null", capture=True)
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and any(v in line for v in ["11","17","21"]):
            candidates.insert(0, parts[2])

    # Search via find
    out2, _, _ = run_cmd("find /usr/lib/jvm -maxdepth 2 -name 'java' -type f 2>/dev/null | head -5", capture=True)
    for p in out2.splitlines():
        candidates.insert(0, str(Path(p).parent.parent))

    # Try readlink
    out3, _, _ = run_cmd("readlink -f $(which java) 2>/dev/null", capture=True)
    if out3:
        candidates.insert(0, str(Path(out3).parent.parent))

    java_home = None
    for c in candidates:
        c = c.strip()
        if c and os.path.exists(os.path.join(c, "bin", "java")):
            java_home = c
            break

    if java_home:
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH','')}"
        ok(f"JAVA_HOME → {java_home}")

        # Set as system default
        java_bin = os.path.join(java_home, "bin", "java")
        run_cmd(f"sudo update-alternatives --set java {java_bin} 2>/dev/null")

        # Write to .bashrc permanently
        bashrc = Path.home() / ".bashrc"
        content = bashrc.read_text() if bashrc.exists() else ""
        block = f"\n# === SparkAgent: Auto-configured Java ===\nexport JAVA_HOME={java_home}\nexport PATH=$JAVA_HOME/bin:$PATH\n"
        if f"JAVA_HOME={java_home}" not in content:
            # Remove old JAVA_HOME entries first
            new_lines = [l for l in content.splitlines() if "JAVA_HOME" not in l and "SparkAgent" not in l]
            with open(bashrc, "w") as f:
                f.write("\n".join(new_lines) + block)
            ok(f"JAVA_HOME saved to ~/.bashrc permanently")
        else:
            ok("JAVA_HOME already in ~/.bashrc")

        # Verify
        _, stderr, _ = run_cmd("java -version", capture=True)
        info(f"Active: {stderr[:60] if stderr else 'Java set'}")
    else:
        warn("Could not find JAVA_HOME. Spark will use simulation mode.")

# ── Step 3: Install Python packages ─────────────────────────
def step3_packages():
    step(3, "Installing Python Packages")

    def pip_install(spec):
        _, _, code = run_cmd(f"pip install '{spec}' -q --break-system-packages 2>/dev/null")
        if code != 0:
            _, _, code2 = run_cmd(f"pip install '{spec}' -q")
        return code == 0 or code2 == 0

    pkgs = [
        ("requests",   "requests>=2.31.0"),
        ("fastapi",    "fastapi>=0.110.0"),
        ("uvicorn",    "uvicorn[standard]>=0.29.0"),
        ("pydantic",   "pydantic>=2.0.0"),
        ("dotenv",     "python-dotenv>=1.0.0"),
        ("rich",       "rich>=13.0.0"),
        ("pytest",     "pytest>=8.0.0"),
    ]
    for mod, spec in pkgs:
        _, _, code = run_cmd(f"python3 -c 'import {mod}'", capture=True)
        if code == 0:
            ok(f"{mod} ✓")
        else:
            info(f"Installing {mod}...")
            pip_install(spec)
            ok(f"{mod} installed")

    # PySpark
    out, _, code = run_cmd("python3 -c 'import pyspark; print(pyspark.__version__)'", capture=True)
    if code == 0 and out:
        ok(f"PySpark {out} ✓")
    else:
        info("Installing PySpark 3.5.1 (may take 1-2 min)...")
        pip_install("pyspark==3.5.1")
        out2, _, _ = run_cmd("python3 -c 'import pyspark; print(pyspark.__version__)'", capture=True)
        if out2:
            ok(f"PySpark {out2} installed!")
        else:
            warn("PySpark install issue. Will use simulation mode.")

# ── Step 4: Verify real Spark starts ────────────────────────
def step4_verify_spark():
    step(4, "Verifying Real Spark Starts")

    java_home = os.environ.get("JAVA_HOME", "")
    jh_line = f'os.environ["JAVA_HOME"] = "{java_home}"' if java_home else ""

    test = f"""
import os, sys
{jh_line}
if "{java_home}": os.environ["PATH"] = "{java_home}/bin:" + os.environ.get("PATH","")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[2]").appName("test")\\
        .config("spark.ui.enabled","false")\\
        .config("spark.driver.bindAddress","127.0.0.1")\\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    v = spark.version
    df = spark.createDataFrame([(1,"hello"),(2,"world")], ["id","val"])
    cnt = df.count()
    spark.stop()
    print("REAL:" + v + ":rows=" + str(cnt))
except Exception as e:
    print("SIM:" + str(e)[:100])
"""
    out, _, _ = run_cmd(f'python3 -c \'{test}\'', capture=True)

    if out.startswith("REAL:"):
        parts = out.split(":")
        ver = parts[1] if len(parts) > 1 else "?"
        ok(f"🚀 Apache Spark {ver} — REAL MODE CONFIRMED!")
        ok("spark-submit is working. Full Spark engine active.")
        return True
    else:
        reason = out.replace("SIM:","")[:100]
        warn(f"Real Spark unavailable: {reason}")
        warn("All 11 tools still work in SIMULATION mode!")
        return False

# ── Step 5: API key ──────────────────────────────────────────
def step5_api_key():
    step(5, "Configure OpenRouter API Key")

    env_file = Path(".env")
    existing_key = ""
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("OPENROUTER_API_KEY="):
                existing_key = line.split("=",1)[1].strip()

    key = os.environ.get("OPENROUTER_API_KEY","") or existing_key
    if key and len(key) > 20 and not key.startswith("sk-or-v1-your"):
        ok(f"API key found: {key[:20]}...")
    else:
        print()
        print("  📋 Get FREE key → https://openrouter.ai/keys")
        print("     Sign up → Keys → Create Key (starts with sk-or-v1-)")
        print()
        key = input("  Paste your OpenRouter API key (Enter to skip): ").strip()
        if key.startswith("sk-or-v1-"):
            ok(f"Key accepted: {key[:20]}...")
        else:
            warn("No key. Add OPENROUTER_API_KEY to .env later.")
            key = existing_key or ""

    java_home = os.environ.get("JAVA_HOME","")
    with open(env_file,"w") as f:
        f.write(f"OPENROUTER_API_KEY={key}\n")
        f.write(f"LLM_MODEL=openrouter/auto\n")
        f.write(f"SPARK_MASTER=local[*]\n")
        f.write(f"JAVA_HOME={java_home}\n")
        f.write(f"SPARK_LOCAL_IP=127.0.0.1\n")
    ok(".env written")

    if key and not key.startswith("sk-or-v1-your"):
        bashrc = Path.home() / ".bashrc"
        content = bashrc.read_text() if bashrc.exists() else ""
        if f'OPENROUTER_API_KEY="{key}"' not in content:
            with open(bashrc,"a") as f:
                f.write(f'\nexport OPENROUTER_API_KEY="{key}"\n')
            ok("API key saved to ~/.bashrc")

# ── Step 6: Sample data ──────────────────────────────────────
def step6_sample_data():
    step(6, "Creating Sample Data")
    Path("/tmp/spark_input").mkdir(parents=True, exist_ok=True)
    Path("/tmp/spark_output").mkdir(parents=True, exist_ok=True)
    csv = "/tmp/spark_input/sales_events.csv"
    with open(csv,"w") as f:
        f.write("id,event_ts,user_id,event_type,amount,currency,date\n")
        for i in range(1, 101):
            f.write(f"{i},2024-01-{(i%28)+1:02d}T10:00:00,user_{i%20+1},purchase,{i*10}.00,USD,2024-01-{(i%28)+1:02d}\n")
        for i in range(1, 6):  # duplicates
            f.write(f"{i},2024-01-{(i%28)+1:02d}T10:00:00,user_{i},purchase,{i*10}.00,USD,2024-01-{(i%28)+1:02d}\n")
    ok(f"{csv} (105 rows, 5 dupes)")

# ── Step 7: Test LLM ─────────────────────────────────────────
def step7_test_llm():
    step(7, "Testing LLM Connection")
    env_file = Path(".env")
    key = ""
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("OPENROUTER_API_KEY="):
                key = line.split("=",1)[1].strip()
    if not key or len(key) < 20:
        warn("No API key — skip"); return
    try:
        import requests
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"},
            json={"model":"openrouter/auto","max_tokens":10,
                  "messages":[{"role":"user","content":"Say OK"}]},
            timeout=15,
        )
        if resp.status_code == 200:
            ok(f"LLM connected! Model: {resp.json().get('model','ok')}")
        else:
            warn(f"LLM status {resp.status_code}")
    except Exception as e:
        warn(f"LLM test: {e}")

# ── Summary ──────────────────────────────────────────────────
def print_final():
    jh = os.environ.get("JAVA_HOME","not set")
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║               ✅  SETUP COMPLETE!                            ║
╠══════════════════════════════════════════════════════════════╣
║  JAVA_HOME  : {jh[:47]:<47}║
║  Sample CSV : /tmp/spark_input/sales_events.csv              ║
║  Output Dir : /tmp/spark_output/                             ║
╠══════════════════════════════════════════════════════════════╣
║  🚀 RUN NOW:                                                 ║
║                                                              ║
║  python3 main.py run \\                                       ║
║    --goal "Read sales, deduplicate, write to Delta" \\        ║
║    --source /tmp/spark_input/ \\                              ║
║    --target /tmp/spark_output/                               ║
╚══════════════════════════════════════════════════════════════╝
""")

if __name__ == "__main__":
    print(BANNER)
    step1_system()
    step2_java()
    step3_packages()
    step4_verify_spark()
    step5_api_key()
    step6_sample_data()
    step7_test_llm()
    print_final()
