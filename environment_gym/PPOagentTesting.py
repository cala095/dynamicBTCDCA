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

def main():
    # Load your data
    data_1m = pd.read_csv('data/merged_data_1m.csv')
    data_1H = pd.read_csv('data/merged_data_1H.csv')
    data_1D = pd.read_csv('data/merged_data_1D.csv')

    # Specify the folder containing the saved model and statistics
    # You can adjust this to point to the folder you want to load
    total_timesteps = 100000  # Should match the one used during training
    base_folder_name = f"ppo_crypto_trading_{total_timesteps}"
    version = 100
    folder_name = base_folder_name

    # If there are multiple versions, adjust to load the correct one
    while os.path.exists(folder_name) or version > 0:
        version -=1
        folder_name = f"{base_folder_name}_v{version}"
        if os.path.exists(folder_name):
            break
        else:
            folder_name = base_folder_name

    if not os.path.exists(base_folder_name):
        print(f"Model folder {base_folder_name} does not exist.")
        return
    else:
        print(f"Loaded {folder_name} + ppo_crypto_trading.py")
    # Create the testing environment
    test_env = CryptoTradingEnv(data_1m, data_1H, data_1D, render_mode='human')
    test_env = DummyVecEnv([lambda: test_env])

    # Load the saved VecNormalize statistics
    vec_norm_path = os.path.join(folder_name, "vec_normalize.pkl")
    test_env = VecNormalize.load(vec_norm_path, test_env)
    test_env.training = False  # Do not update normalization statistics
    test_env.norm_reward = False  # Do not normalize rewards during testing

    # Load the trained model
    model_path = os.path.join(folder_name, "ppo_crypto_trading")
    model = PPO.load(model_path, env=test_env)

    obs = test_env.reset()
    done = False
    while not done:
        action, _states = model.predict(obs)
        obs, reward, done, info = test_env.step(action)
        test_env.render()
        # Retrieve and print the last monitor data
        monitor_df = test_env.envs[0].unwrapped.get_monitor_data()
        print(monitor_df.tail(1))
        # time.sleep(0.5)  # Sleep to slow down the rendering

if __name__ == "__main__":
    main()
