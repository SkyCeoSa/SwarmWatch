import os
import time
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def record_demo():
    print("Initializing headless Chrome...")
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--window-size=1280,800')
    options.add_argument('--force-device-scale-factor=1')
    options.add_argument('--hide-scrollbars')

    driver = webdriver.Chrome(options=options)
    
    # Path to local dashboard.html
    current_dir = os.path.abspath(os.getcwd())
    file_url = f"file:///{current_dir.replace(os.sep, '/')}/dashboard.html"
    print(f"Loading {file_url}...")
    driver.get(file_url)
    time.sleep(1)

    frames = []
    temp_dir = os.path.join(current_dir, "temp_gif_frames")
    os.makedirs(temp_dir, exist_ok=True)

    def capture_frame(label, hold_count=1):
        idx = len(frames)
        path = os.path.join(temp_dir, f"frame_{idx:03d}_{label}.png")
        driver.save_screenshot(path)
        im = Image.open(path).convert("RGB")
        # Resize slightly for optimal crispness and low file size (1024x640)
        im_resized = im.resize((1024, 640), Image.Resampling.LANCZOS)
        for _ in range(hold_count):
            frames.append(im_resized)
        print(f"Captured {label} (total frames: {len(frames)})")

    # 1. Calm Initial State (hold 3 frames ~ 1.5s)
    capture_frame("initial_calm", hold_count=3)

    # 2. Start simulation
    try:
        btn_sim = driver.find_element(By.ID, "btnSimulate")
        btn_sim.click()
        print("Clicked Run Swarm Simulation")
    except Exception as e:
        print("Error clicking simulate:", e)

    # Capture streaming turns (approx 8 frames across ~4 seconds)
    for i in range(8):
        time.sleep(0.4)
        capture_frame(f"turn_streaming_{i+1}")

    # Hold on tripped circuit state (hold 3 frames ~ 1.5s)
    capture_frame("circuit_tripped_hold", hold_count=3)

    # 3. Switch to Topology DAG view
    try:
        btn_dag = driver.find_element(By.ID, "btnViewDag")
        btn_dag.click()
        time.sleep(0.5)
        capture_frame("topology_dag_view", hold_count=3)
    except Exception as e:
        print("Error clicking DAG view:", e)

    # 4. Switch back to Stream view
    try:
        btn_stream = driver.find_element(By.ID, "btnViewStream")
        btn_stream.click()
        time.sleep(0.3)
        capture_frame("stream_return", hold_count=2)
    except Exception as e:
        print("Error clicking Stream view:", e)

    # 5. Step back with Time-Machine scrubber
    try:
        btn_back = driver.find_element(By.ID, "btnStepBack")
        btn_back.click()
        time.sleep(0.3)
        capture_frame("time_machine_step_back_1", hold_count=2)
        btn_back.click()
        time.sleep(0.3)
        capture_frame("time_machine_step_back_2", hold_count=3)
    except Exception as e:
        print("Error clicking time-machine back:", e)

    driver.quit()

    print(f"Total frame sequence count: {len(frames)}")
    if not frames:
        print("No frames captured!")
        return

    # Compile into animated GIF with adaptive 128-color palette
    output_gif = os.path.join("docs", "assets", "swarmwatch_demo.gif")
    print(f"Quantizing and saving GIF to {output_gif}...")

    # Duration per frame: ~350ms
    frames[0].save(
        output_gif,
        save_all=True,
        append_images=frames[1:],
        optimize=True,
        duration=350,
        loop=0
    )

    size_mb = os.path.getsize(output_gif) / (1024 * 1024)
    print(f"GIF generated successfully! Size: {size_mb:.2f} MB at {output_gif}")

    # Cleanup temp frames
    for f in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, f))
    os.rmdir(temp_dir)

if __name__ == "__main__":
    record_demo()
