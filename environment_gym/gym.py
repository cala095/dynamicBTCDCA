import gym
from gym import spaces
import numpy as np

class CryptoTradingEnv(gym.Env):
    """
    environment with multiple time frames and portfolio management.
    """
    metadata = {'render.modes': ['human']}

    def __init__(
        self,
        data_1m,
        data_1h,
        data_1d,
        balance_range=(100, 1000000),
        episode_length_range=(60, 1000000),
    ):
        super(CryptoTradingEnv, self).__init__()

        # Store data for different time frames
        self.data_1m = data_1m  # DataFrame with 1-minute data
        self.data_1h = data_1h  # DataFrame with 1-hour data
        self.data_1d = data_1d  # DataFrame with 1-day data

        # Environment parameters
        self.balance_range = balance_range
        self.episode_length_range = episode_length_range
        self.initial_balance = None
        self.current_step = None
        self.end_step = None
        self.portfolio = None

        # Define observation space
        n_features_1m = self.data_1m.shape[1]
        n_features_1h = self.data_1h.shape[1]
        n_features_1d = self.data_1d.shape[1]
        n_portfolio_features = 3  # balance, btc holdings, avg_price

        self.observation_space = spaces.Box(
            low=-np.inf,
            high=np.inf,
            shape=(n_features_1m + n_features_1h + n_features_1d + n_portfolio_features,),
            dtype=np.float32
        )

        # Define action space
        # Buy Amount: Continuous value representing USD to spend on BTC (min $10)
        # Trend Predictions: Continuous value between -1 and +1
        # Guess Minimum Predictions: Continuous value representing predicted price minima (in USD)
        self.action_space = spaces.Box(
            low=np.array([10.0, -1.0, 0.0]),
            high=np.array([np.inf, 1.0, np.inf]),
            dtype=np.float32
        )

    def reset(self, initial_balance=None, episode_length=None):
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
            self.current_step = 0
        else:
            self.current_step = np.random.randint(0, max_start_step)

        self.end_step = self.current_step + self.max_steps

        # Initialize portfolio
        self.portfolio = {'balance': self.initial_balance, 'btc': 0, 'avg_price': 0}

        return self._next_observation()

    def _next_observation(self):
        # Get data from different time frames at the current step
        obs_1m = self.data_1m.iloc[self.current_step].values
        obs_1h = self.data_1h.iloc[self.current_step].values
        obs_1d = self.data_1d.iloc[self.current_step].values

        # Portfolio state
        portfolio_state = np.array([
            self.portfolio['balance'],
            self.portfolio['btc'],
            self.portfolio['avg_price']
        ])

        # Combine all observations into a single flattened array
        observation = np.concatenate((obs_1m, obs_1h, obs_1d, portfolio_state)).astype(np.float32)

        return observation

    def step(self, action):
        # Execute one time step within the environment

        # Unpack the action components
        buy_amount = action[0]
        trend_prediction = action[1]
        min_price_prediction = action[2]

        # Apply constraints
        buy_amount = max(10.0, min(buy_amount, self.portfolio['balance']))

        # Update portfolio based on the action
        self._execute_trade(buy_amount)

        # Advance the market to the next step
        self.current_step += 1

        # Calculate the reward
        reward = self._calculate_reward(trend_prediction, min_price_prediction)

        # Check if the episode is done
        done = self.current_step >= self.end_step

        # Additional info (optional)
        info = {}

        # Get the next observation
        if not done:
            obs = self._next_observation()
        else:
            obs = None

        return obs, reward, done, info

    def _execute_trade(self, buy_amount):
        # Simulate buying BTC with the specified amount
        current_price = self.data_1m.iloc[self.current_step]['Close']  # Current BTC price

        # Calculate the amount of BTC bought
        btc_bought = buy_amount / current_price

        # Update portfolio
        total_cost = btc_bought * current_price
        self.portfolio['balance'] -= total_cost
        self.portfolio['btc'] += btc_bought

        # Update average price
        total_spent = (self.portfolio['avg_price'] * (self.portfolio['btc'] - btc_bought)) + total_cost
        self.portfolio['avg_price'] = total_spent / self.portfolio['btc'] if self.portfolio['btc'] > 0 else 0

    def _calculate_reward(self, trend_prediction, min_price_prediction):
        # Implement the reward function based on your specified components

        reward = 0.0

        # Accumulation Reward: Reward for accumulating BTC at a lower average price
        current_price = self.data_1m.iloc[self.current_step]['Close']
        btc_avg_price = self.portfolio['avg_price']
        market_avg_price = self.data_1m['Close'][self.current_step:self.end_step].mean()

        price_difference = market_avg_price - btc_avg_price
        accumulation_reward = max(0, price_difference)  # Positive reward if below market average
        reward += accumulation_reward

        # Prediction Accuracy Reward: Based on trend prediction accuracy
        actual_trend = self._get_actual_trend()
        trend_accuracy = 1 - abs(trend_prediction - actual_trend)
        reward += trend_accuracy

        # Timing Reward: For buying near predicted or actual minima
        actual_min_price = self.data_1m['Close'][self.current_step:self.end_step].min()
        min_price_difference = abs(min_price_prediction - actual_min_price)
        timing_reward = max(0, 1 - (min_price_difference / actual_min_price))
        reward += timing_reward

        # Penalties
        # Penalty for being over the market average price
        if btc_avg_price > market_avg_price:
            penalty = btc_avg_price - market_avg_price
            reward -= penalty
        # Penalty for failing to invest funds
        if self.portfolio['balance'] > 0 and self.current_step >= self.end_step:
            uninvested_penalty = self.portfolio['balance'] * 0.1  # Penalize 10% of uninvested balance
            reward -= uninvested_penalty

        return reward

    def _get_actual_trend(self):
        # Calculate the actual market trend between the current step and the end of the episode
        start_price = self.data_1m.iloc[self.current_step]['Close']
        end_price = self.data_1m.iloc[self.end_step - 1]['Close']
        actual_trend = (end_price - start_price) / start_price
        actual_trend = np.clip(actual_trend, -1.0, 1.0)
        return actual_trend

    def render(self, mode='human'):
        # Render the environment to the screen (optional)
        profit = self.portfolio['btc'] * self.data_1m.iloc[self.current_step]['Close'] + self.portfolio['balance'] - self.initial_balance
        print(f'Step: {self.current_step}')
        print(f'Balance: {self.portfolio["balance"]}')
        print(f'BTC Holdings: {self.portfolio["btc"]}')
        print(f'Avg Buy Price: {self.portfolio["avg_price"]}')
        print(f'Profit: {profit}')

    def close(self):
        # Clean up (optional)
        pass
