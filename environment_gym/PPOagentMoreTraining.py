import os
import gym
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import SubprocVecEnv, VecNormalize
from stable_baselines3.common.logger import configure
from stable_baselines3.common.callbacks import EvalCallback
from stable_baselines3.common.vec_env import DummyVecEnv
from myEnv import CryptoTradingEnv
import numpy as np
import pandas as pd
import logging

def make_env(data_1m, data_1H, data_1D, testing = False):
    def _init():
        env = CryptoTradingEnv(data_1m, data_1H, data_1D, render_mode=None, testing = testing)
        return env
    return _init

def main():
    # Set the total_timesteps for additional training
    additional_timesteps = 100_000  # Adjust as needed
    original_timestep = 30_000     # Adjust based on your existing model's folder name
    # Specify the base folder name used during saving
    base_folder_name = f"ppo_crypto_trading_{original_timestep}"#_continued_{original_timestep-additional_timesteps}"  #################### EDIT THIS ##########################

    # Find the latest version of the saved model
    version = 1                                                         ###################### ALSO THIS ###################
    folder_name = base_folder_name

    while os.path.exists(folder_name):
        last_existing_folder = folder_name
        folder_name = f"{base_folder_name}_v{version}"
        version += 1
    
    folder_name = last_existing_folder
    print(f"Loading model and VecNormalize statistics from folder: {folder_name}")

    # Check if the folder exists
    if not os.path.exists(folder_name):
        print(f"Model folder {folder_name} does not exist.")
        return

    # Load your data
    data_1m = pd.read_csv('data/merged_data_1m.csv')
    data_1H = pd.read_csv('data/merged_data_1H.csv')
    data_1D = pd.read_csv('data/merged_data_1D.csv')

    # Number of environments to run in parallel for training
    num_envs = 2  # Adjust based on your CPU cores

    # Create the vectorized environment for training
    env = SubprocVecEnv([make_env(data_1m, data_1H, data_1D) for _ in range(num_envs)])

    # Load the VecNormalize statistics
    vec_normalize_path = os.path.join(folder_name, "vec_normalize.pkl")
    if os.path.exists(vec_normalize_path):
        env = VecNormalize.load(vec_normalize_path, env)
    else:
        print(f"VecNormalize file not found at {vec_normalize_path}")
        return

    env.training = True  # Enable training mode to update statistics
    env.norm_reward = True  # Keep reward normalization consistent

    # Load the trained model
    model_path = os.path.join(folder_name, "ppo_crypto_trading.zip")
    if os.path.exists(model_path):
        model = PPO.load(model_path, env=env, device='cpu')
    else:
        print(f"Model file not found at {model_path}")
        return

    # # Modify hyperparameters as needed
    # model.n_steps = 2048  # This can be modified if `n_steps * n_envs` is divisible by `batch_size`
    # model.batch_size = 1024
    # model.learning_rate = 0.0003  # Adjusted learning rate
    # model.n_epochs = 20  # Adjusted number of epochs
    # model.clip_range = 0.2
    # model.ent_coef = 0.01
    # # Note: Since we cannot change policy_kwargs (network architecture), we do not modify it

    # Create the testing environment
    test_env = CryptoTradingEnv(data_1m, data_1H, data_1D, testing = True)
    test_env = DummyVecEnv([lambda: test_env])
    test_env = VecNormalize.load(vec_normalize_path, test_env)
    test_env.training = False  # Do not update normalization statistics
    test_env.norm_reward = False  # Do not normalize rewards during testing
    

    # Use the custom callback
    evalfrq = 8_000
    eval_callback = EvalCallback(
        test_env,
        best_model_save_path=None,
        eval_freq=evalfrq,
        log_path=os.path.join(folder_name, f"evalfrq_{evalfrq}_eval_{original_timestep+additional_timesteps}.log"),
        n_eval_episodes=10,
        deterministic=True,
        render=False,
        verbose=1 
    )
    
    # Continue training the model and integrate the eval_callback
    model.learn(total_timesteps=additional_timesteps, callback=eval_callback)

    # Versioning: Save the new model and VecNormalize statistics in a new folder
    new_base_folder_name = f"ppo_crypto_trading_{original_timestep+additional_timesteps}_{version}_continued_{original_timestep}"
    new_version = 1
    new_folder_name = new_base_folder_name

    while os.path.exists(new_folder_name):
        new_folder_name = f"{new_base_folder_name}_v{new_version}"
        new_version += 1

    os.makedirs(new_folder_name)

    # Save the updated model
    model.save(os.path.join(new_folder_name, "ppo_crypto_trading"))
    # Save the updated VecNormalize statistics
    env.save(os.path.join(new_folder_name, "vec_normalize.pkl"))

    print(f"Updated model and normalization statistics saved in folder: {new_folder_name}")

if __name__ == "__main__":
    main()
