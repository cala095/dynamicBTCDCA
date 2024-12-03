import warnings
warnings.filterwarnings("ignore")

import gym
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from myEnv import CryptoTradingEnv
import time
import numpy as np


# Now, create the main function
def main():
    # Load your data here (data_1m, data_1H, data_1D)
    # For the example, I'll create dummy data
    import pandas as pd

    # Create dummy data
    # num_steps = 10000
    data_1m = pd.read_csv('data/merged_data_1m.csv')
    data_1H = pd.read_csv('data/merged_data_1H.csv')
    data_1D = pd.read_csv('data/merged_data_1D.csv')

    # Initialize the environment with render_mode
    env = CryptoTradingEnv(data_1m, data_1H, data_1D, render_mode='human')

    # Wrap it in a DummyVecEnv
    env = DummyVecEnv([lambda: env])

    # Wrap the environment with VecNormalize
    env = VecNormalize(env, norm_obs=True, norm_reward=True, clip_obs=10.)

    # Create the PPO model
    model = PPO('MlpPolicy', env, verbose=1, device='cuda')

    # Train the agent
    # model.learn(total_timesteps=1000000)

    # After training, you can test the agent
    obs = env.reset()
    done = False
    while not done:
        action, _states = model.predict(obs)
        obs, reward, done, info = env.step(action)
        env.render()
        # Optionally, retrieve the monitor data
        monitor_df = env.envs[0].unwrapped.get_monitor_data()
        print(monitor_df)
        time.sleep(0.5)  # Sleep for 1 second to slow down the rendering

    

if __name__ == "__main__":
    main()
