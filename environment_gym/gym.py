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
        data_1H,
        data_1D,
        balance_range=(100, 1000000),
        episode_length_range=(60, 86400),
    ):
        super(CryptoTradingEnv, self).__init__()

        # Store data for different time frames
        self.data_1m = data_1m  # DataFrame with 1-minute data
        self.data_1H = data_1H  # DataFrame with 1-hour data
        self.data_1D = data_1D  # DataFrame with 1-day data

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
        n_features_1m = self.data_1m.shape[1]
        n_features_1h = self.data_1H.shape[1]
        n_features_1d = self.data_1D.shape[1]
        n_portfolio_features = 3  # balance, btc holdings, avg_price

        # Define possible columns
        # self.price_columns = ['Open', 'High', 'Low', 'Close'] #etc: we can also count all the column in the dataset
        # self.num_columns = n_features_1m #len(self.price_columns)

        # Define window size range
        # self.min_window_size = 1
        # self.max_window_size = 10  # Adjust as needed -> the model must learn to use the different time-frame so more than 10y is useless

        # this define the possi
        self.observation_space = spaces.Dict({
            'time_frame_1m': spaces.Box(low=-np.inf, high=np.inf, shape=(n_features_1m,), dtype=np.float32),
            'time_frame_1h': spaces.Box(low=-np.inf, high=np.inf, shape=(n_features_1h,), dtype=np.float32),
            'time_frame_1d': spaces.Box(low=-np.inf, high=np.inf, shape=(n_features_1d,), dtype=np.float32),
            'portfolio_state': spaces.Box(low=-np.inf, high=np.inf, shape=(n_portfolio_features,), dtype=np.float32)
            'progress_to_epEnd': spaces.Box(low=0.0, high=1.0, shape=(1,), dtype=np.float32)
        })

        # Define action space
        # Buy Amount: Continuous value representing USD to spend on BTC (min $10)
        # Trend Predictions: Continuous value between -1 and +1
        # Guess Minimum Predictions: Continuous value representing predicted price minima (in USD)
        # Column Selection for the features
        # Window size
        # Define action space
        self.action_space = spaces.Tuple((
            # Buy Amount: Continuous value representing USD to spend on BTC (min $10)
            spaces.Box(low=10.0, high=np.inf, shape=(1,), dtype=np.float32),
            # Trend Prediction: Continuous value between -1 and +1
            spaces.Box(low=-1.0, high=1.0, shape=(1,), dtype=np.float32),
            # Guess Minimum Predictions: Continuous value representing predicted price minima (in USD)
            spaces.Box(low=0.0, high=np.inf, shape=(1,), dtype=np.float32),
            # # Column Selection for the features: Discrete value representing column index
            # spaces.Discrete(self.num_columns),
            # # Window Size: Discrete value representing window size
            # spaces.Discrete(self.max_window_size - self.min_window_size + 1),
        ))

    # Initialize timeframes + current_step on each one
    def updMarketState(self):
        datetime_at_current_step = self.data_1m['Formatted_Time'].iloc[current_step]
        self.current_step_1m = self.current_step
        one_hour_format = datetime_at_current_step.floor('H')  # Round down to the hour
        day_format = datetime_at_current_step.date()  # Get the date only
        end_of_week_date = datetime_at_current_step + pd.DateOffset(days=(6 - datetime_at_current_step.weekday())) # Calculate the end of the current week (next Sunday)
        month_format = datetime_at_current_step + pd.offsets.MonthEnd(0)  # End of the current month
        # On the time-frames different then 1 minute we need to adjust the index
        self.data_1H.set_index('Formatted_Time', inplace=True)
        self.current_step_1H = self.data_1H.index.get_loc(one_hour_format)
        self.data_1D.set_index('Formatted_Time', inplace=True)
        self.current_step_1D = self.data_1D.index.get_loc(day_format)
        #TODO add week and year
        #DEBUG
        print("current_step_1m (also current_step in this version):", self.current_step_1m)        
        print("datetime_at_current_step:", datetime_at_current_step)
        print("One Hour Format:", one_hour_format)
        print("Index for 1H:", self.current_step_1H)
        print("One Day Format:", day_format)
        print("Index for 1D:", self.current_step_1D)


    def reset(self, initial_balance=100, episode_length=None): #we use 100 as initial balance so that we can use percentage of founds later
        # Reset the state of the environment to an initial state

        # Randomize initial balance if not provided
        if initial_balance is None:
            self.initial_balance = np.random.uniform(*self.balance_range)
        else:
            self.initial_balance = initial_balance

        # Randomize episode length if not provided
        if episode_length is None:
            self.max_steps = np.random.randint(*self.episode_length_range) # max_steps is a random number inside the range
        else:
            self.max_steps = episode_length

        # Ensure max_steps does not exceed data length
        self.max_steps = int(min(self.max_steps, len(self.data_1m) - 1)) # For future updates

        # Randomize starting point in data
        max_start_step = len(self.data_1m) - self.max_steps
        if max_start_step <= 0:
            # self.current_step = 0 # If max_start_step is less than zero we start from the beginning 
            print("something strange happened, max_start_step was <= 0") # Should never happen
            exit(0)
        else:
            self.start_step = np.random.randint(0, max_start_step)
            self.current_step = self.start_step

        self.end_step = self.current_step + self.max_steps

        updMarketState()

        # Initialize portfolio
        self.portfolio = {'balance': self.initial_balance, 'btc': 0, 'avg_price': 0}

        return self._next_observation()

    def _next_observation(self):
        # Get data for each time frame
        #TODO remove headers, add all the files to one dataframe for timeframe
        obs_1m = self.data_1m.iloc[self.current_step_1m].values.astype(np.float32)
        obs_1h = self.data_1H.iloc[self.current_step_1H].values.astype(np.float32)
        obs_1d = self.data_1D.iloc[self.current_step_1D].values.astype(np.float32)

        # Portfolio state
        portfolio_state = np.array([
            self.portfolio['balance'],
            self.portfolio['btc'],
            self.portfolio['avg_price']
        ], dtype=np.float32)

        # Calculate progress towards the end of the episode
        progress = (self.current_step - self.start_step) / (self.end_step - self.start_step)
        progress = np.clip(progress, 0.0, 1.0).astype(np.float32)

        # Construct the observation dictionary
        observation = {
            'time_frame_1m': obs_1m,
            'time_frame_1h': obs_1h,
            'time_frame_1d': obs_1d,
            'portfolio_state': portfolio_state
            'progress_to_epEnd': np.array([progress], dtype=np.float32)
        }
        return observation

    def step(self, action):
        # Unpack the action components
        buy_amount = action[0][0]  # Extract scalar from array
        trend_prediction = action[1][0]
        min_price_prediction = action[2][0]
        # column_choice = action[3]
        # window_size_choice = action[4]

        # Apply constraints on buy amount
        if buy_amount > 0:
            buy_amount = max(1.0, min(buy_amount, self.initial_balance/10)#self.portfolio['balance']))
            # Update portfolio based on the action
            self._execute_trade(buy_amount)
            reward += self._calculate_reward(trend_prediction, min_price_prediction, buy_amount)
        else:
            buy_amount = 0

        # Map column_choice to selected column
        # selected_column = self.price_columns[column_choice]

        # Map window_size_choice to actual window size
        # trend_window = window_size_choice + self.min_window_size  # Adjust for offset if min_window_size > 0

        # Advance the market to the next step
        self.current_step += 1
        updMarketState()

        # Check if the episode is done
        done = self.current_step >= self.end_step

        # Get the next observation
        if not done:
            obs = self._next_observation()
        else:
            # Calculate the reward
            reward += self._calculate_reward(trend_prediction, min_price_prediction, 0) #, selected_column, trend_window)
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

    def _calculate_reward(self, trend_prediction, min_price_prediction, buyed) #, selected_column, trend_window):
        reward = 0.0

        # # Timing Reward: For buying near predicted or actual minima (VERSION FOR MULTI COLUMN OPTIONS, MAYBE USELESS)
        # past_min_price = self.data_1m["selected_column"][max(0, self.current_step - trend_window):self.current_step+1].min()
        # min_price_difference = abs(min_price_prediction - past_min_price)
        # timing_reward = max(0, 1 - (min_price_difference / past_min_price))
        # reward += timing_reward
        # Timing Reward: For buying near predicted or actual minima
        if buyed == True: # TO INCENTIVE BUYING
            # actual_min_price = self.data_1m['Close'][self.current_step:self.end_step].min() # MAYBE TO DEBUG CAN BE USEFULL, DONT USE THIS HAS FUTURE KNOWLEDGE IN IT
            current_min_price = self.data_1m['Close'][self.start_step:self.current_step].min() #TODO SHOULD BE THE 'LOW' COLUMN, BUT FOR THE MOMENT WE ARE HAPPY WITH THIS
            prediction_price_difference = abs(min_price_prediction - current_min_price)
            timing_reward = max(0, 1 - (prediction_price_difference / current_min_price)) * buyed
            reward += timing_reward
        else: #AT EPISODE END -> #TODO refactor this function
            # Accumulation Reward: Reward for accumulating BTC at a lower average price
            current_price = self.data_1m.iloc[self.current_step]['Close']
            btc_avg_price = self.portfolio['avg_price'] #TODO this can be zero... in that case i want to punish him for not buying (lets see if subctrating the balance works or traumatize him)
            if btc_avg_price == 0:
                reward -= self.portfolio['balance']
            else:
                market_avg_price = self.data_1m['Close'][self.start_step:end_index].mean() # Slice the 'Close' column from start_step to start_step + end_step
                price_difference = market_avg_price - btc_avg_price
                if price_difference >= 0 # btc_avg_price is lower
                    reward += price_difference * portfolio # Positive reward if below market average
                else
                    reward -= price_difference # Negative reward if over mrt avg

            # Prediction Accuracy Reward: Based on trend prediction accuracy
            actual_trend = self._get_actual_trend()
            trend_accuracy = 1 - abs(trend_prediction - actual_trend)
            reward += trend_accuracy

            

            # Penalties
            # Penalty for being over the market average price
            if btc_avg_price > market_avg_price:
                penalty = btc_avg_price - market_avg_price
                reward -= penalty
            # Extra Penalty for failing to invest funds
            # if self.portfolio['balance'] > 0 and self.current_step >= self.end_step:
            #     uninvested_penalty = self.portfolio['balance'] * 0.1
            #     reward -= uninvested_penalty

            return reward

    # def _get_actual_trend(self, selected_column, trend_window):
    #     start_idx = max(0, self.current_step - trend_window)
    #     start_price = self.data_1m.iloc[start_idx][selected_column]
    #     end_price = self.data_1m.iloc[self.current_step][selected_column]

    #     actual_trend = (end_price - start_price) / start_price
    #     actual_trend = np.clip(actual_trend, -1.0, 1.0)
    #     return actual_trend
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
        print(f'Balance: ${self.portfolio["balance"]:.2f}')
        print(f'BTC Holdings: {self.portfolio["btc"]:.6f} BTC')
        print(f'Avg Buy Price: ${self.portfolio["avg_price"]:.2f}')
        print(f'Profit: ${profit:.2f}')

    def close(self):
        # Clean up (optional)
        pass
