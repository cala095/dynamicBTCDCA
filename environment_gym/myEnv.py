import gym
from gym import spaces
import numpy as np
import pandas as pd

class CryptoTradingEnv(gym.Env):
    """
    Environment with multiple time frames and portfolio management.
    """
    metadata = {'render_modes': ['human']}

    def __init__(
        self,
        data_1m,
        data_1H,
        data_1D,
        balance_range=(100, 1000000),
        episode_length_range=(60, 43200),
        render_mode=None
    ):
        super(CryptoTradingEnv, self).__init__()

        # Store the render mode
        self.render_mode = render_mode

        # Store data for different time frames
        self.data_1m = data_1m  # DataFrame with 1-minute data
        self.data_1H = data_1H  # DataFrame with 1-hour data
        self.data_1D = data_1D  # DataFrame with 1-day data

        #monitor
        self.monitor_progress = 0
        self.episode_reward = 0
        self.model_reward = 0

        # Environment parameters
        self.balance_range = balance_range
        self.episode_length_range = episode_length_range
        self.initial_balance = None
        self.start_step = None
        self.current_step = None
        self.current_step_1m = None
        self.current_step_1H = None
        self.current_step_1D = None
        self.end_step = None
        self.portfolio = None
        

        # Define observation space
        n_features_1m = self.data_1m.shape[1] - 1  # Exclude 'Formatted_Time'
        n_features_1h = self.data_1H.shape[1] - 1
        n_features_1d = self.data_1D.shape[1] - 1
        n_portfolio_features = 3  # balance, btc holdings, avg_price
        n_progress_feature = 1  # progress_to_epEnd

        # Total features
        total_features = n_features_1m + n_features_1h + n_features_1d + n_portfolio_features + n_progress_feature

        self.observation_space = spaces.Box(
            low=-1.0, high=1.0, shape=(total_features,), dtype=np.float32
        )

        # Define action space scaled between -1 and 1
        self.action_space = spaces.Box(
            low=-1.0,
            high=1.0,
            shape=(3,),
            dtype=np.float32
        )

        # For monitoring
        self.monitor_data = []

    # Initialize timeframes + current_step on each one
    def updMarketState(self):
        datetime_at_current_step = pd.to_datetime(self.data_1m['Formatted_Time'].iloc[self.current_step])
        self.current_step_1m = self.current_step
        one_hour_format = datetime_at_current_step.floor('H')  # Round down to the hour
        day_format = datetime_at_current_step.date()  # Get the date only

        # On the time-frames different than 1 minute we need to adjust the index
        matching_row_1H = self.data_1H[self.data_1H['Formatted_Time'] == str(one_hour_format)]
        matching_row_1D = self.data_1D[self.data_1D['Formatted_Time'] == str(day_format)]
        if matching_row_1H.empty or matching_row_1D.empty:
            print("date conversion broke (matching_row_1H/matching_row_1D...)")
            print(matching_row_1H)
            print(one_hour_format)
            print(matching_row_1D)
            print(day_format)
        self.current_step_1H = matching_row_1H.index[0]
        self.current_step_1D = matching_row_1D.index[0]

    def reset(self, initial_balance=100, episode_length=None):
        # Reset the state of the environment to an initial state

        # Randomize initial balance if not provided
        if initial_balance is None:
            self.initial_balance = np.random.uniform(*self.balance_range)
        else:
            self.initial_balance = initial_balance

        # Randomize episode length if not provided
        if episode_length is None:
            self.max_steps = np.random.randint(*self.episode_length_range)
        else:
            self.max_steps = episode_length

        # Ensure max_steps does not exceed data length
        self.max_steps = int(min(self.max_steps, len(self.data_1m) - 1))

        # Randomize starting point in data
        max_start_step = len(self.data_1m) - self.max_steps
        if max_start_step <= 0:
            print("Something strange happened, max_start_step was <= 0")
            exit(0)
        else:
            self.start_step = np.random.randint(0, max_start_step)
            self.current_step = self.start_step

        self.end_step = self.current_step + self.max_steps

        self.updMarketState()

        # Initialize portfolio
        self.portfolio = {'balance': self.initial_balance, 'btc': 0, 'avg_price': 0}

        # Reset monitor data
        self.monitor_data = []

        return self._next_observation()

    def _next_observation(self):

        # Get data for each time frame
        # Get all columns except 'Formatted_Time' for each observation
        obs_1m = self.data_1m.loc[self.current_step_1m, self.data_1m.columns != 'Formatted_Time'].values.astype(np.float32)
        obs_1h = self.data_1H.loc[self.current_step_1H, self.data_1H.columns != 'Formatted_Time'].values.astype(np.float32)
        obs_1d = self.data_1D.loc[self.current_step_1D, self.data_1D.columns != 'Formatted_Time'].values.astype(np.float32)

        # Normalize observations
        obs_1m = self._normalize_observation(obs_1m)
        obs_1h = self._normalize_observation(obs_1h)
        obs_1d = self._normalize_observation(obs_1d)

        # Portfolio state
        portfolio_state = np.array([
            self.portfolio['balance'],
            self.portfolio['btc'],
            self.portfolio['avg_price']
        ], dtype=np.float32)

        # Normalize portfolio state
        portfolio_state = self._normalize_observation(portfolio_state)

        # Calculate progress towards the end of the episode
        progress = (self.current_step - self.start_step) / (self.end_step - self.start_step)
        progress = np.clip(progress, 0.0, 1.0).astype(np.float32)
        self.monitor_progress = progress

        # Concatenate all observations into a single flat array
        observation = np.concatenate([
            obs_1m,
            obs_1h,
            obs_1d,
            portfolio_state,
            np.array([progress], dtype=np.float32)
        ])

        # Replace NaN values with zeros
        # observation = np.nan_to_num(observation, nan=0.0)
        # observation = np.where(observation is None, 0.0, observation)
        # Clip observation to avoid numerical issues
        # observation = np.clip(observation, -1.0, 1.0)
        

        return observation

    def _normalize_observation(self, obs):
        # Simple normalization to [-1, 1]
        max_abs_obs = np.abs(obs).max()
        if max_abs_obs > 0:
            return obs / max_abs_obs
        else:
            return obs

    def step(self, action):

        # Rescale actions back to original ranges
        buy_amount = ((action[0] + 1) * 100) / 2 # Mapping from [-1, 1] to [1, 100]
        trend_prediction = action[1]  # Already in [-1,1], assuming trend is between -1 and 1
        min_price_prediction = ((action[2] + 1) / 2) * 1e6  # Maps [-1,1] to [0,1e6]

        reward = 0.0  # Initialize reward

        # Apply constraints on buy amount
        if buy_amount > 0 and self.portfolio['balance'] >= buy_amount:
            buy_amount = max(1.0, min(buy_amount, self.initial_balance / 10)) #TODO punish for going over 1/10
            # Update portfolio based on the action
            self._execute_trade(buy_amount)
            buyed = True
            reward += self._calculate_reward(trend_prediction, min_price_prediction, buyed)
        else:
            if self.portfolio['balance'] <= buy_amount: # Removing buy_amount if the agent tries to buy when there are insuffiecent founds
                reward -= buy_amount
            buy_amount = 0
            buyed = False

        # Advance the market to the next step
        self.current_step += 1
        if self.current_step >= len(self.data_1m):
            done = True
            obs = None
        else:
            self.updMarketState()
            done = self.current_step >= self.end_step

            # Get the next observation
            obs = self._next_observation() #if not done else None

        # If episode is done, calculate final reward
        if done:
            reward += self._calculate_reward(trend_prediction, min_price_prediction, buyed=False)

        info = {}  # Additional info

        # Call the monitor function
        self.monitor(action, reward, done)

        return obs, reward, done, info

    def _execute_trade(self, buy_amount):
        # Simulate buying BTC with the specified amount
        current_price = self.data_1m.iloc[self.current_step]['BTC1m_Close']  # Current BTC price

        # Calculate the amount of BTC bought
        btc_bought = buy_amount / current_price

        # Update portfolio
        total_cost = btc_bought * current_price
        self.portfolio['balance'] -= total_cost
        self.portfolio['btc'] += btc_bought

        # Update average price
        total_spent = (self.portfolio['avg_price'] * (self.portfolio['btc'] - btc_bought)) + total_cost
        self.portfolio['avg_price'] = total_spent / self.portfolio['btc'] if self.portfolio['btc'] > 0 else 0

    def _calculate_reward(self, trend_prediction, min_price_prediction, buyed):
        
        reward = 0.0   
        # Stats
        current_price = self.data_1m.iloc[self.current_step]['BTC1m_Close']
        current_min_price = self.data_1m['BTC1m_Close'][self.start_step:self.current_step].min() # current_min_price encountered in the episode
        prediction_price_accuracy = abs(min_price_prediction - current_min_price) + 1  # How much distance there is beetwen what the model think will be the minimum and what the minimum at that moment is 
        buy_accuracy = abs(current_price - current_min_price) # Zero is max but breaks the reward model (current price will always be higher or equal to current_min_price) -> 1 is now max 

        if buyed: # If the agent buys we reward him based on quantity buyed and distance from the current min price encountered, at epEnd we will punish him if he didn't buy or buyed at high prices!
            timing_accuracy_reward = max(0, 1 - (buy_accuracy / (prediction_price_accuracy)) * buyed # If he buyed close to the prediction and the prediction is also good we reward by his conviction!
            reward += timing_accuracy_reward
        else:
            #prediction reward
            if current_step > 0:
                reward -= prediction_price_accuracy  # In this way i'm always punishing him, even if he get the right minimum -> forcing him to buy at the right time (punishing inaction) 
            
            # btc_avg_price = self.portfolio['avg_price']
            # if btc_avg_price == 0:
            #     reward -= self.portfolio['balance']
            btc_avg_price = self.portfolio['avg_price']
            if btc_avg_price != 0:
                market_avg_price = self.data_1m['BTC1m_Close'][self.start_step:self.end_step].mean()
                price_difference = ((market_avg_price - btc_avg_price)/market_avg_price) * 100  # We avoid big numbers difference in btc by using %
                reward += price_difference * (100 - self.portfolio['balance']) # This reward is negative when market_avg_price is lower than btc_avg_price -> again reward conviction

            #TODO implement trend prediction reward
            # actual_trend = self._get_actual_trend()
            # trend_accuracy = 1 - abs(trend_prediction - actual_trend)
            # reward += trend_accuracy

            #this is useless now
            # if btc_avg_price > market_avg_price:
            #     penalty = btc_avg_price - market_avg_price
            #     reward -= penalty * self.portfolio['btc']
            # if self.portfolio['balance'] > 0:
            #     uninvested_penalty = self.portfolio['balance'] * 0.1
            #     reward -= uninvested_penalty

        return reward

    def _get_actual_trend(self):
        # Calculate the actual market trend between the current step and the end of the episode
        start_price = self.data_1m.iloc[self.start_step]['BTC1m_Close']
        end_price = self.data_1m.iloc[self.end_step - 1]['BTC1m_Close']
        actual_trend = (end_price - start_price) / start_price
        actual_trend = np.clip(actual_trend, -1.0, 1.0)
        return actual_trend

    def monitor(self, action, reward, done):
        if done:
            self.episode_reward = 0
        else:
            self.episode_reward += reward
            self.model_reward += reward
        # Record the relevant data
        data = {
            'step': self.current_step,
            'datetime': self.data_1m.iloc[self.current_step - 1]['Formatted_Time'],
            'balance': self.portfolio['balance'],
            'btc_holdings': self.portfolio['btc'],
            'avg_buy_price': self.portfolio['avg_price'],
            'current_price': self.data_1m.iloc[self.current_step - 1]['BTC1m_Close'],
            'profit': self.portfolio['btc'] * self.data_1m.iloc[self.current_step - 1]['BTC1m_Close'] + self.portfolio['balance'] - self.initial_balance,
            'buy_amount': ((action[0] + 1) * 100) / 2, # Maps [-1,1] to [0,100]
            'trend_prediction': action[1],
            'min_price_prediction': ((action[2] + 1) / 2) * 1e6,  # Maps [-1,1] to [0,1e6]
            'step_reward': reward,
            'episode_reward': self.episode_reward,
            'model_reward': self.model_reward,
            'progress': self.monitor_progress
        }
        self.monitor_data.append(data)

    def render(self, mode='human'):
        # Render the environment to the screen (optional)
        current_price = self.data_1m.iloc[self.current_step - 1]['BTC1m_Close']
        profit = self.portfolio['btc'] * current_price + self.portfolio['balance'] - self.initial_balance
        print(f'Step: {self.current_step}')
        print(f'Balance: ${self.portfolio["balance"]:.2f}')
        print(f'BTC Holdings: {self.portfolio["btc"]:.6f} BTC')
        print(f'Avg Buy Price: ${self.portfolio["avg_price"]:.2f}')
        print(f'Current Price: ${current_price:.2f}')
        print(f'Profit: ${profit:.2f}')

    def close(self):
        # Clean up (optional)
        pass

    def get_monitor_data(self):
        return pd.DataFrame(self.monitor_data).tail(1)
