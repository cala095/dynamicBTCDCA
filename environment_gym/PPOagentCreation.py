import os
import gym
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import SubprocVecEnv, VecNormalize
from myEnv import CryptoTradingEnv
import numpy as np
import pandas as pd

def make_env(data_1m, data_1H, data_1D):
    def _init():
        env = CryptoTradingEnv(data_1m, data_1H, data_1D, render_mode=None)
        return env
    return _init

def main():
    # Load your data
    data_1m = pd.read_csv('data/merged_data_1m.csv')
    data_1H = pd.read_csv('data/merged_data_1H.csv')
    data_1D = pd.read_csv('data/merged_data_1D.csv')

    # Number of environments to run in parallel
    num_envs = 6  # Adjust based on your CPU cores

    # Create the vectorized environment
    env = SubprocVecEnv([make_env(data_1m, data_1H, data_1D) for _ in range(num_envs)])

    # Wrap the environment with VecNormalize # a good idea to avoid overfitting on normalizing the whole dataset?  i think yes because btc will be the one changing the most 
    env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=1.0) #TODO maybe implement positive rewards? instead of negative ones?

    # Define a more powerful neural network architecture
    policy_kwargs = dict(
        net_arch=[dict(pi=[256, 256, 128], vf=[256, 256, 128])]
    )

    # Specify total timesteps
    total_timesteps = 20000000  # Adjust as needed
    #n_steps=43200 + total_timesteps = 10 000 000 -> 499s per total_timesteps | 259200 -> 5,34h
    # Create the PPO model with adjusted hyperparameters
    model = PPO(
        'MlpPolicy',
        env,
        verbose=1,
        device='cpu',
        n_steps=2048,
        batch_size=1024,
        learning_rate =0.0003, #standard 0.0003
        n_epochs=20, #standard 20
        policy_kwargs=policy_kwargs,
        clip_range=0.2,
        ent_coef=0.01
    )

    # Train the agent
    model.learn(total_timesteps=total_timesteps)

    # Versioning: Save the model and VecNormalize statistics in a folder
    base_folder_name = f"ppo_crypto_trading_{total_timesteps}"
    version = 1
    folder_name = base_folder_name

    while os.path.exists(folder_name):
        folder_name = f"{base_folder_name}_v{version}"
        version += 1

    os.makedirs(folder_name)

    # Save the model
    model.save(os.path.join(folder_name, "ppo_crypto_trading"))
    # Save the VecNormalize statistics
    env.save(os.path.join(folder_name, "vec_normalize.pkl"))

    print(f"Model and normalization statistics saved in folder: {folder_name}")

if __name__ == "__main__":
    main()
