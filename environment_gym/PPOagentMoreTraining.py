import os
import gym
import logging
import numpy as np
import pandas as pd

from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import SubprocVecEnv, VecNormalize, DummyVecEnv
from stable_baselines3.common.logger import configure
from stable_baselines3.common.callbacks import EvalCallback

from myEnv import CryptoTradingEnv

def make_env(data_1m, data_1H, data_1D, testing=False):
    """
    Factory function to create a CryptoTradingEnv instance.
    `testing=True` makes the environment log additional info, if applicable.
    """
    def _init():
        env = CryptoTradingEnv(data_1m, data_1H, data_1D, render_mode=None, testing=testing)
        return env
    return _init

def main():
    # Additional training timesteps
    additional_timesteps = 200_000  # Adjust as needed
    original_timestep = 30_000      # Adjust based on your existing model's folder name

    # Base folder name for loading the model and VecNormalize stats
    base_folder_name = f"ppo_crypto_trading_{original_timestep}"

    # Find the latest version of the saved model (incrementing vX until no folder is found)
    version = 1
    folder_name = base_folder_name
    last_existing_folder = None

    while os.path.exists(folder_name):
        last_existing_folder = folder_name
        folder_name = f"{base_folder_name}_v{version}"
        version += 1

    # Use the last existing folder where the model was found
    folder_name = last_existing_folder
    if folder_name is None:
        print(f"No existing model folder found for {base_folder_name}.")
        return

    print(f"Loading model and VecNormalize statistics from folder: {folder_name}")

    # Check if folder exists
    if not os.path.exists(folder_name):
        print(f"Model folder {folder_name} does not exist.")
        return

    # Load the data
    data_1m = pd.read_csv('data/merged_data_1m.csv')
    data_1H = pd.read_csv('data/merged_data_1H.csv')
    data_1D = pd.read_csv('data/merged_data_1D.csv')

    # Number of parallel environments for training
    num_envs = 12  # Adjust based on CPU cores

    # Create the training environment
    env = SubprocVecEnv([make_env(data_1m, data_1H, data_1D) for _ in range(num_envs)])

    # Load VecNormalize statistics
    vec_normalize_path = os.path.join(folder_name, "vec_normalize.pkl")
    if not os.path.exists(vec_normalize_path):
        print(f"VecNormalize file not found at {vec_normalize_path}")
        return

    env = VecNormalize.load(vec_normalize_path, env)
    env.training = True
    env.norm_reward = True

    # Load the trained model
    model_path = os.path.join(folder_name, "ppo_crypto_trading.zip")
    if not os.path.exists(model_path):
        print(f"Model file not found at {model_path}")
        return

    model = PPO.load(model_path, env=env, device='cuda')
    print("Model loaded successfully.")

    # Create a single-environment evaluation setup using DummyVecEnv
    # test_env_instance = CryptoTradingEnv(data_1m, data_1H, data_1D, testing=True)
    # test_env = DummyVecEnv([lambda: test_env_instance])
    # test_env = VecNormalize.load(vec_normalize_path, test_env)
    # test_env.training = False
    # test_env.norm_reward = False

    # # Evaluation frequency and callback
    # eval_freq = 35_000  # Evaluate every 8k steps
    # eval_log_path = os.path.join(folder_name, f"evalfrq_{eval_freq}_eval_{original_timestep + additional_timesteps}.log")

    # eval_callback = EvalCallback(
    #     test_env,
    #     best_model_save_path=None,
    #     eval_freq=eval_freq,
    #     log_path=eval_log_path,
    #     n_eval_episodes=10,
    #     deterministic=True,
    #     render=False,
    #     verbose=1
    # )

    # Train the model further with the EvalCallback
    print(f"Starting additional training for {additional_timesteps} timesteps...")
    model.learn(total_timesteps=additional_timesteps)#, callback=eval_callback)
    print("Additional training completed.")

    # Versioning for saving the updated model and normalization stats
    new_base_folder_name = f"ppo_crypto_trading_{original_timestep + additional_timesteps}_{version}_continued_{original_timestep}"
    new_version = 1
    new_folder_name = new_base_folder_name

    while os.path.exists(new_folder_name):
        new_folder_name = f"{new_base_folder_name}_v{new_version}"
        new_version += 1

    os.makedirs(new_folder_name, exist_ok=True)

    # Save the updated model
    model.save(os.path.join(new_folder_name, "ppo_crypto_trading"))
    # Save the updated VecNormalize statistics
    env.save(os.path.join(new_folder_name, "vec_normalize.pkl"))

    print(f"Updated model and normalization statistics saved in folder: {new_folder_name}")

if __name__ == "__main__":
    main()
