# mines/mines.py
import asyncio
import random
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter
from database import user_exists, get_user_balance, update_user_balance
from utils import logger, send_with_retry

# Game configurations
GRID_SIZE = 5
MIN_MINES = 1
MAX_MINES = 24

MULTIPLIERS = {
    1: [1.03, 1.07, 1.12, 1.17, 1.23, 1.30, 1.37, 1.45, 1.54, 1.64, 1.76, 1.89, 2.05, 2.23, 2.46, 2.74, 3.08, 3.52, 4.10, 4.93, 6.16, 8.21, 12.31, 24.63],
    2: [1.07, 1.17, 1.28, 1.41, 1.56, 1.73, 1.92, 2.14, 2.40, 2.69, 3.05, 3.48, 4.00, 4.65, 5.46, 6.50, 7.84, 9.62, 12.07, 15.50, 20.53, 28.25, 41.29],
    3: [1.12, 1.25, 1.39, 1.56, 1.75, 1.98, 2.24, 2.56, 2.94, 3.40, 3.96, 4.66, 5.54, 6.67, 8.16, 10.16, 12.91, 16.81, 22.55, 31.32, 45.42, 70.38],
    4: [1.18, 1.35, 1.53, 1.74, 1.98, 2.26, 2.60, 3.01, 3.52, 4.14, 4.92, 5.91, 7.18, 8.85, 11.06, 14.13, 18.53, 25.14, 35.37, 52.28, 83.54],
    5: [1.23, 1.45, 1.67, 1.93, 2.23, 2.58, 3.01, 3.53, 4.18, 4.99, 6.03, 7.38, 9.17, 11.57, 14.89, 19.62, 26.60, 37.42, 55.62, 90.09],
    6: [1.30, 1.56, 1.85, 2.19, 2.58, 3.05, 3.62, 4.32, 5.18, 6.25, 7.60, 9.33, 11.57, 14.53, 18.53, 24.13, 32.19, 44.06, 62.50, 92.59],
    7: [1.37, 1.67, 2.03, 2.46, 2.97, 3.58, 4.32, 5.23, 6.37, 7.80, 9.62, 11.96, 15.03, 19.18, 24.90, 33.01, 45.05, 63.49, 92.59],
    8: [1.45, 1.80, 2.23, 2.74, 3.36, 4.10, 5.00, 6.13, 7.55, 9.33, 11.64, 14.64, 18.64, 24.13, 31.75, 42.68, 58.82, 83.33],
    9: [1.54, 1.93, 2.46, 3.05, 3.77, 4.65, 5.76, 7.14, 8.85, 11.06, 13.89, 17.54, 22.55, 29.41, 39.06, 52.91, 73.53],
    10: [1.64, 2.08, 2.69, 3.40, 4.23, 5.26, 6.58, 8.20, 10.28, 12.94, 16.39, 20.83, 26.79, 35.09, 46.73, 63.49],
    11: [1.76, 2.25, 2.94, 3.77, 4.76, 5.95, 7.46, 9.33, 11.76, 14.89, 18.87, 24.13, 31.25, 41.10, 55.56],
    12: [1.89, 2.46, 3.23, 4.18, 5.32, 6.71, 8.47, 10.64, 13.51, 17.24, 22.06, 28.57, 37.31, 49.50],
    13: [2.05, 2.69, 3.57, 4.65, 5.95, 7.55, 9.62, 12.19, 15.50, 19.84, 25.64, 33.33, 43.86],
    14: [2.23, 2.94, 3.96, 5.18, 6.67, 8.47, 10.87, 13.89, 17.86, 23.08, 30.03, 39.47],
    15: [2.46, 3.23, 4.41, 5.76, 7.46, 9.62, 12.50, 16.13, 20.83, 27.17, 35.71],
    16: [2.74, 3.57, 4.92, 6.45, 8.47, 11.06, 14.53, 19.05, 25.00, 33.33],
    17: [3.08, 4.00, 5.54, 7.30, 9.62, 12.82, 17.24, 23.26, 31.58],
    18: [3.52, 4.55, 6.25, 8.33, 11.11, 15.15, 20.83, 28.99],
    19: [4.10, 5.26, 7.14, 9.62, 13.16, 18.18, 25.64],
    20: [4.93, 6.25, 8.47, 11.76, 16.67, 23.81],
    21: [6.16, 7.69, 10.64, 15.38, 23.08],
    22: [8.21, 10.00, 14.29, 22.22],
    23: [12.31, 15.38, 25.00],
    24: [24.63]
}

COMMON_AREAS = [(0,0), (0,4), (4,0), (4,4), (2,2)]

def generate_grid(m, win_streak, extra_mines=2):
    """
    Generate a 5x5 grid with exactly `total_mines` unique mine positions.
    
    Args:
        m (int): Number of mines chosen by the player (1 to 24).
        win_streak (int): Player's win streak to adjust weights (unused in this fix).
        extra_mines (int): Number of extra mines to add (default 2).
    
    Returns:
        tuple: (grid, mine_positions) where grid is the 5x5 game grid and mine_positions is a list of mine coordinates.
    """
    total_mines = min(m + extra_mines, GRID_SIZE * GRID_SIZE)
    all_positions = [(i, j) for i in range(GRID_SIZE) for j in range(GRID_SIZE)]
    weights = [10 if pos in COMMON_AREAS else 1 for pos in all_positions]
    mine_positions = []
    remaining_positions = all_positions.copy()
    remaining_weights = weights.copy()
    while len(mine_positions) < total_mines and remaining_positions:
        pos = random.choices(remaining_positions, weights=remaining_weights, k=1)[0]
        mine_positions.append(pos)
        index = remaining_positions.index(pos)
        remaining_positions.pop(index)
        remaining_weights.pop(index)
    grid = [[{'type': 'mine' if (i, j) in mine_positions else 'safe', 'revealed': False} 
             for j in range(GRID_SIZE)] for i in range(GRID_SIZE)]
    return grid, mine_positions

def get_potential_winnings(game):
    if game['safe_revealed'] == 0:
        return 0
    return game['bet_amount'] * game['total_multiplier']

def generate_grid_buttons(game, reveal_all=False):
    grid = game['grid']
    user_id = game['user_id']
    revealed_mines = game.get('revealed_mines', []) if reveal_all else []
    if reveal_all and game['game_over']:
        if 'hit_mine_position' in game:
            revealed_mines = [game['hit_mine_position']] + random.sample(
                [p for p in game['all_mine_positions'] if p != game['hit_mine_position']], 
                min(game['m'] - 1, len(game['all_mine_positions']) - 1)
            )
        else:
            revealed_mines = random.sample(game['all_mine_positions'], min(game['m'], len(game['all_mine_positions'])))
        game['revealed_mines'] = revealed_mines
    
    grid_buttons = []
    for i in range(GRID_SIZE):
        row = []
        for j in range(GRID_SIZE):
           
            tile = grid[i][j]
            if reveal_all or tile['revealed']:
                if tile['type'] == 'mine' and (i, j) in revealed_mines:
                    text = "💣"
                elif tile['type'] == 'safe' and 'multiplier' in tile:
                    text = f"{tile['multiplier']:.2f}x"
                else:
                    text = "?"
            else:
                text = "?"
            callback_data = f"mine_choose_{i}_{j}_{user_id}"
            row.append(InlineKeyboardButton(text, callback_data=callback_data))
        grid_buttons.append(row)
    return grid_buttons

def get_persistent_buttons(game):
    user_id = game['user_id']
    mine_change_counter = game.get('mine_change_counter', 0)
    if game['state'] == 'setup':
        return [
            [InlineKeyboardButton("⬅️", callback_data=f"mine_left_{user_id}"),
             InlineKeyboardButton(f"💣 {game['m']}", callback_data=f"mine_noop_{mine_change_counter}_{user_id}"),
             InlineKeyboardButton("➡️", callback_data=f"mine_right_{user_id}")],
            [InlineKeyboardButton("▶️ Start Game", callback_data=f"mine_startgame_{user_id}")],
            [InlineKeyboardButton("📜 Rules", callback_data=f"mine_rules_{user_id}")]
        ]
    elif game['state'] == 'playing' and not game['game_over']:
        return [
            [InlineKeyboardButton("💰 Cash Out", callback_data=f"mine_cashout_{user_id}")],
            [InlineKeyboardButton("📜 Rules", callback_data=f"mine_rules_{user_id}")]
        ]
    else:
        return [
            [InlineKeyboardButton("▶️ Start Game", callback_data=f"mine_startgame_{user_id}")],
            [InlineKeyboardButton("📜 Rules", callback_data=f"mine_rules_{user_id}")]
        ]

async def mine_command(update, context):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    args = context.args

    if not user_exists(user_id):
        await send_with_retry(context.bot, chat_id, "Please register with /start.")
        return

    if len(args) != 1:
        await send_with_retry(context.bot, chat_id, "Usage: /mine <amount>\nExample: /mine 1")
        return

    try:
        bet_amount = float(args[0])
        if bet_amount <= 0:
            raise ValueError("Bet must be positive.")
        balance = get_user_balance(user_id)
        if bet_amount > balance:
            await send_with_retry(context.bot, chat_id, f"Insufficient balance! You have ${balance:.2f}.")
            return

        game = {
            'user_id': user_id,
            'bet_amount': bet_amount,
            'm': 1,
            'state': 'setup',
            'grid': None,
            'all_mine_positions': [],
            'revealed_mines': [],
            'message_id': None,
            'mine_change_counter': 0,
            'game_over': False,
            'safe_revealed': 0,
            'total_multiplier': 0.0,
            'ended_text': None
        }
        context.user_data['mine_game'] = game

        text = f"💣 Mine Game for {update.effective_user.mention_html()} - Bet: ${bet_amount:.2f}\n\nChoose number of mines:"
        keyboard = get_persistent_buttons(game)
        message = await send_with_retry(
            context.bot, chat_id, text,
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
        )
        game['message_id'] = message.message_id
    except ValueError as e:
        await send_with_retry(context.bot, chat_id, f"Invalid bet: {str(e)}. Use a positive number.")

async def mine_button_handler(update, context):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith('mine_'):
        return

    parts = data.split('_')
    action = parts[1]
    chat_id = query.message.chat_id
    user_id = query.from_user.id

    if action in ['left', 'right', 'startgame', 'cashout', 'rules', 'back']:
        callback_user_id = int(parts[2])
    elif action == 'choose':
        i, j, callback_user_id = int(parts[2]), int(parts[3]), int(parts[4])
    elif action == 'noop':
        return
    else:
        logger.warning(f"Unknown action: {action}")
        return

    if callback_user_id != user_id:
        await query.answer("This is not your game!", show_alert=True)
        return

    game = context.user_data.get('mine_game')
    if not game or game['message_id'] != query.message.message_id:
        await query.edit_message_text("No active Mine Game! Start with /mine <amount>.")
        return

    async def edit_message_with_retry(text, keyboard):
        try:
            await context.bot.edit_message_text(
                text, chat_id=chat_id, message_id=game['message_id'],
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
            )
        except RetryAfter as e:
            await query.answer(f"Please wait {e.retry_after} seconds.", show_alert=True)
            await asyncio.sleep(e.retry_after)
            await context.bot.edit_message_text(
                text, chat_id=chat_id, message_id=game['message_id'],
                reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
            )

    if action == 'startgame' and game['state'] in ['setup', 'ended']:
        balance = get_user_balance(user_id)
        if balance < game['bet_amount']:
            await send_with_retry(context.bot, chat_id, f"Insufficient balance! You have ${balance:.2f}.")
            return
        update_user_balance(user_id, balance - game['bet_amount'])
        win_streak = context.user_data.get('win_streak', 0)
        grid, all_mine_positions = generate_grid(game['m'], win_streak)
        game['grid'] = grid
        game['all_mine_positions'] = all_mine_positions
        game['revealed_mines'] = []
        game['state'] = 'playing'
        game['game_over'] = False
        game['safe_revealed'] = 0
        game['total_multiplier'] = 0.0
        text = (f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n"
                f"Mines: {game['m']}\n"
                f"Total Multiplier: 0.00x\n"
                f"Potential Winnings: $0.00")
        keyboard = generate_grid_buttons(game) + get_persistent_buttons(game)
        await edit_message_with_retry(text, keyboard)
    elif action == 'choose' and game['state'] == 'playing' and not game['game_over']:
        tile = game['grid'][i][j]
        if not tile['revealed']:
            tile['revealed'] = True
            if tile['type'] == 'mine':
                game['game_over'] = True
                game['state'] = 'ended'
                game['hit_mine_position'] = (i, j)
                context.user_data['win_streak'] = 0
                text = (f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n"
                        f"Mines: {game['m']}\n\n"
                        f"💥 Boom! You hit a mine and lost your bet.")
                game['ended_text'] = text
                keyboard = generate_grid_buttons(game, reveal_all=True) + get_persistent_buttons(game)
            else:
                game['safe_revealed'] += 1
                multipliers = MULTIPLIERS.get(game['m'], [1.0] * 25)
                multiplier = multipliers[min(game['safe_revealed'] - 1, len(multipliers) - 1)]
                tile['multiplier'] = multiplier
                game['total_multiplier'] = multiplier
                potential_winnings = get_potential_winnings(game)
                text = (f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n"
                        f"Mines: {game['m']}\n"
                        f"Total Multiplier: {game['total_multiplier']:.2f}x\n"
                        f"Potential Winnings: ${potential_winnings:.2f}")
                keyboard = generate_grid_buttons(game) + get_persistent_buttons(game)
            await edit_message_with_retry(text, keyboard)
    elif action == 'cashout' and game['state'] == 'playing' and not game['game_over']:
        potential_winnings = get_potential_winnings(game)
        balance = get_user_balance(user_id)
        new_balance = balance + potential_winnings
        update_user_balance(user_id, new_balance)
        game['game_over'] = True
        game['state'] = 'ended'
        context.user_data['win_streak'] = context.user_data.get('win_streak', 0) + 1
        text = (f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n"
                f"Mines: {game['m']}\n\n"
                f"💰 Cashed out!\n"
                f"Total Multiplier: {game['total_multiplier']:.2f}x\n"
                f"Winnings: ${potential_winnings:.2f}\n"
                f"New Balance: ${new_balance:.2f}")
        game['ended_text'] = text
        keyboard = generate_grid_buttons(game, reveal_all=True) + get_persistent_buttons(game)
        await edit_message_with_retry(text, keyboard)
    elif action in ['left', 'right'] and game['state'] == 'setup':
        if action == 'left':
            game['m'] = max(MIN_MINES, game['m'] - 1)
        elif action == 'right':
            game['m'] = min(MAX_MINES, game['m'] + 1)
        game['mine_change_counter'] += 1
        text = f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n\nChoose number of mines:"
        keyboard = get_persistent_buttons(game)
        await edit_message_with_retry(text, keyboard)
    elif action == 'rules':
        rules_text = (
            "💣 Mine Game Rules 💣\n\n"
            "• Grid: 5x5 tiles.\n"
            "• Choose 1 to 24 mines.\n"
            "• Uncover safe tiles to increase your multiplier.\n"
            "• Hit a mine (💣) and lose your bet.\n"
            "• Cash out anytime to secure winnings!"
        )
        keyboard = [[InlineKeyboardButton("⬅️ Back", callback_data=f"mine_back_{user_id}")]]
        await edit_message_with_retry(rules_text, keyboard)
    elif action == 'back':
        if game['state'] == 'setup':
            text = f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n\nChoose number of mines:"
            keyboard = get_persistent_buttons(game)
        elif game['state'] == 'playing':
            potential_winnings = get_potential_winnings(game)
            text = (f"💣 Mine Game for {query.from_user.mention_html()} - Bet: ${game['bet_amount']:.2f}\n"
                    f"Mines: {game['m']}\n"
                    f"Total Multiplier: {game['total_multiplier']:.2f}x\n"
                    f"Potential Winnings: ${potential_winnings:.2f}")
            keyboard = generate_grid_buttons(game) + get_persistent_buttons(game)
        elif game['state'] == 'ended':
            text = game['ended_text']
            keyboard = get_persistent_buttons(game)
        await edit_message_with_retry(text, keyboard)