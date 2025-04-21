import subprocess

# List of Python scripts to run
scripts = [
    "CoreMaking.py",
    "SandProcessing.py",
    "Moulding.py",
    "Melting.py",
    "Melt_prod.py",
    "Laddle.py",
    "PostProcessing.py",
    "Auxiliary.py"
]

# Run all scripts in parallel
processes = [subprocess.Popen(["python", script]) for script in scripts]

# Wait for all processes to complete
for p in processes:
    p.wait()

print("All Scripts Executed Successfully!")
