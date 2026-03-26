import subprocess, shutil, os

if os.path.exists("/tmp/seed-data"):
    shutil.rmtree("/tmp/seed-data")

subprocess.run(
    ["git", "clone", "https://github.com/diego-opsera/seed-data.git", "/tmp/seed-data"],
    check=True
)
print("Done.")
