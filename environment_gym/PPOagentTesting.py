import warnings
warnings.filterwarnings("ignore")

import os
import gym
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from myEnv import CryptoTradingEnv
import numpy as np
import pandas as pd
import time
import logging

def main():
    # Specify the folder containing the saved model and statistics
    total_timesteps = 20000000  # Should match the one used during training
    base_folder_name = f"ppo_crypto_trading_{total_timesteps}"
    version = 100
    folder_name = base_folder_name

    # If there are multiple versions, adjust to load the correct one
    while os.path.exists(base_folder_name) and version > 0:
        version -=1
        folder_name = f"{base_folder_name}_v{version}"
        print(folder_name)
        if os.path.exists(folder_name):
            break
        else:
            folder_name = base_folder_name

    if not os.path.exists(base_folder_name):
        print(f"Model folder {base_folder_name} does not exist.")
        return
    else:
        print(f"Loaded {folder_name} + ppo_crypto_trading.py")
    
    # **Set up logging**
    # Define the model name and log file path
    model_name = "ppo_crypto_trading"
    log_file_path = os.path.join(folder_name, f"{model_name}_output.log")

    # Create a logger object
    logger = logging.getLogger('model_logger')
    logger.setLevel(logging.INFO)

    # Create a file handler that logs messages to the specified file
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(logging.INFO)

    # Create a console handler to output logs to the console (optional)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Define a formatter and set it for both handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    # Load your data
    data_1m = pd.read_csv('data/merged_data_1m.csv')
    data_1H = pd.read_csv('data/merged_data_1H.csv')
    data_1D = pd.read_csv('data/merged_data_1D.csv')

    # Create the testing environment
    test_env = CryptoTradingEnv(data_1m, data_1H, data_1D, render_mode='human')
    test_env = DummyVecEnv([lambda: test_env])

    # Load the saved VecNormalize statistics
    vec_norm_path = os.path.join(folder_name, "vec_normalize.pkl")
    test_env = VecNormalize.load(vec_norm_path, test_env)
    test_env.training = False  # Do not update normalization statistics
    test_env.norm_reward = False  # Do not normalize rewards during testing

    # Load the trained model
    model_path = os.path.join(folder_name, model_name)
    model = PPO.load(model_path, env=test_env)

    # **Adjust Pandas display options for better formatting**
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.float_format', '{:,.6f}'.format)

    obs = test_env.reset()
    done = False
    while not done:
        action, _states = model.predict(obs)
        obs, reward, done, info = test_env.step(action)
        test_env.render()

        # Retrieve the last monitor data
        monitor_df = test_env.envs[0].unwrapped.get_monitor_data()
        last_monitor_entry = monitor_df.tail(1)

        # Convert the DataFrame to a string for better formatting
        formatted_entry = last_monitor_entry.to_string(index=False)

        # Log the formatted last monitor data
        logger.info(f"\n{formatted_entry}\n")

        # Optionally, sleep to slow down the rendering (uncomment if needed)
        # time.sleep(0.5)

    # Close the logging handlers
    logger.removeHandler(fh)
    fh.close()
    logger.removeHandler(ch)
    ch.close()

if __name__ == "__main__":
    main()
