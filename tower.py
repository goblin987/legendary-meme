import random
import asyncio
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter
from database import user_exists, get_user_balance, update_user_balance
from utils import logger, send_with_retry

# Game configurations
MODE_CONFIG = {
    'Easy': 4,
    'Medium': 3,
    'Hard': 2
}
MODES = ['Easy', 'Medium', 'Hard']

MULTIPLIERS = {
    'Easy': [1.31, 1.74, 2.32, 3.10, 4.13, 5.51, 7.34, 9.79, 13.05],
    'Medium': [1.47, 2.21, 3.31, 4.96, 7.44, 11.16, 16.74, 25.11, 37.67],
    'Hard': [1.72, 3.68, 7.84, 14.68, 30.36, 62.72, 125.44, 250.88, 501.76]
}

def get_potential_winnings(game):
    if game['current_level'] > 0:
        multiplier = MULTIPLIERS[game['chosen_mode']][game['current_level'] - 1]
        return game['bet_amount'] * multiplier
    return 0

def generate_grid_buttons(game, reveal_all=False):
    columns = MODE_CONFIG[game['chosen_mode']]
    current_level = game['current_level']
    revealed = game['revealed']
    monkey_positions = game['monkey_positions']
    grid_buttons = []

    for row in range(8, -1, -1):
        row_buttons = []
        for col in range(columns):
            if reveal_all or game['game_over']:
                is_monkey = col == monkey_positions[row]
                if row == 8:
                    emoji = "🐒" if is_monkey else "🍌"
                else:
                    emoji = "🐒" if is_monkey else "🌴"
                if col == revealed[row]:
                    row_buttons.append(InlineKeyboardButton(f"> {emoji} <", callback_data="noop"))
                else:
                    row_buttons.append(InlineKeyboardButton(emoji, callback_data="noop"))
            elif row < current_level:
                if col == revealed[row]:
                    row_buttons.append(InlineKeyboardButton("> 🌴 <", callback_data="noop"))
                else:
                    row_buttons.append(InlineKeyboardButton(" ", callback_data="noop"))
            elif row == current_level:
                if current_level == 8:
                    emoji = "🍌"
                else:
                    emoji = "🟩"
                if revealed[row] is not None and col == revealed[row]:
                    row_buttons.append(InlineKeyboardButton(f"> {emoji} <", callback_data="noop"))
                else:
                    row_buttons.append(InlineKeyboardButton(emoji, callback_data=f"tower_choose_{col}_{row}"))
            else:
                row_buttons.append(InlineKeyboardButton(" ", callback_data="noop"))
        grid_buttons.append(row_buttons)
    return grid_buttons

def get_persistent_buttons(game):
    mode_change_counter = game.get('mode_change_counter', 0)
    if game['state'] == 'playing' and not game['game_over'] and game['current_level'] > 0:
        action_button = [InlineKeyboardButton("Cash Out", callback_data="tower_cash_out")]
    else:
        action_button = [InlineKeyboardButton("Start Game", callback_data="tower_start_game")]
    return [
        [
            InlineKeyboardButton("⬅️", callback_data="tower_left"),
            InlineKeyboardButton(game['chosen_mode'], callback_data=f"noop_{mode_change_counter}"),
            InlineKeyboardButton("➡️", callback_data="tower_right")
        ],
        action_button,
        [InlineKeyboardButton("Rules", callback_data="tower_rules")]
    ]

async def tower_command(update, context):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    args = context.args

    if not user_exists(user_id):
        await send_with_retry(context.bot, chat_id, text="Please register with /start.")
        return

    if len(args) != 1:
        await send_with_retry(context.bot, chat_id, text="Usage: /tower <amount>\nExample: /tower 1")
        return

    try:
        bet_amount = float(args[0])
        if bet_amount <= 0:
            raise ValueError("Bet must be positive.")

        game = {
            'bet_amount': bet_amount,
            'chosen_mode': 'Medium',
            'state': 'setup',
            'current_level': -1,
            'monkey_positions': None,
            'extra_monkeys': [[] for _ in range(9)],
            'revealed': [None] * 9,
            'game_over': False,
            'message_id': None,
            'mode_change_counter': 0,
            'ended_text': None
        }
        context.user_data['tower_game'] = game

        balance = get_user_balance(user_id)
        text = f"🐒 Monkey Tower\n\nBet: ${bet_amount:.2f}\nBalance: ${balance:.2f}\n\nChoose game mode:"
        keyboard = generate_grid_buttons(game) + get_persistent_buttons(game)
        message = await send_with_retry(context.bot, chat_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
        game['message_id'] = message.message_id
    except ValueError as e:
        await send_with_retry(context.bot, chat_id, text=f"Invalid bet amount: {str(e)}. Use a positive number.")

async def tower_button_handler(update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    data = query.data
    logger.info(f"Handling tower button: {data}")

    game = context.user_data['tower_game']
    message_id = game['message_id']
    balance = get_user_balance(user_id)

    if data == 'tower_rules':
        rules_text = (
            f"🐒 Monkey Tower\n\n"
            f"Bet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\n"
            "Rules:\n"
            "• Choose mode: Easy (4 tiles), Medium (3), Hard (2).\n"
            "• Pick a safe spot each level.\n"
            "• Avoid monkeys (🐒) or lose.\n"
            "• Reach level 9 for bananas (🍌) and big wins!"
        )
        keyboard = [[InlineKeyboardButton("Back", callback_data="tower_back")]]
        await context.bot.edit_message_text(rules_text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == 'tower_back':
        if game['state'] == 'setup':
            text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\nChoose game mode:"
        elif game['state'] == 'playing':
            potential_winnings = get_potential_winnings(game)
            text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\nLevel {game['current_level'] + 1}: Choose a spot\nPotential Cash-Out: ${potential_winnings:.2f} USDT"
        elif game['state'] == 'ended':
            text = game['ended_text']
        keyboard = generate_grid_buttons(game, reveal_all=game['game_over']) + get_persistent_buttons(game)
        await context.bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data in ['tower_left', 'tower_right'] and game['state'] in ['setup', 'ended']:
        current_index = MODES.index(game['chosen_mode'])
        new_index = (current_index + (-1 if data == 'tower_left' else 1)) % 3
        game['chosen_mode'] = MODES[new_index]
        game['mode_change_counter'] += 1
        text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\nChoose game mode:" if game['state'] == 'setup' else game['ended_text']
        keyboard = generate_grid_buttons(game, reveal_all=game['game_over']) + get_persistent_buttons(game)
        await context.bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == 'tower_start_game' and game['state'] in ['setup', 'ended']:
        balance = get_user_balance(user_id)
        if balance < game['bet_amount']:
            text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\nInsufficient balance to start!"
            await send_with_retry(context.bot, chat_id, text=text)
            return
        update_user_balance(user_id, balance - game['bet_amount'])
        game['state'] = 'playing'
        game['current_level'] = 0
        columns = MODE_CONFIG[game['chosen_mode']]
        game['monkey_positions'] = [random.randint(0, columns - 1) for _ in range(9)]
        game['extra_monkeys'] = [[] for _ in range(9)]

        if game['chosen_mode'] == 'Easy':
            for level in range(5, 9):
                available_cols = [c for c in range(4) if c != game['monkey_positions'][level]]
                if level == 8:
                    extra = random.sample(available_cols, 2)
                else:
                    extra = random.sample(available_cols, 1)
                game['extra_monkeys'][level] = extra
        elif game['chosen_mode'] == 'Medium':
            for level in range(4, 9):
                available_cols = [c for c in range(3) if c != game['monkey_positions'][level]]
                extra = random.sample(available_cols, 1)
                game['extra_monkeys'][level] = extra
        elif game['chosen_mode'] == 'Hard':
            for level in range(3, 9):
                other_col = 1 - game['monkey_positions'][level]
                game['extra_monkeys'][level] = [other_col]

        game['revealed'] = [None] * 9
        game['game_over'] = False
        text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance - game['bet_amount']:.2f}\n\nLevel 1: Choose a spot"
        keyboard = generate_grid_buttons(game) + get_persistent_buttons(game)
        await context.bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if game['state'] == 'playing':
        if data.startswith("tower_choose_"):
            if game['game_over']:
                return
            parts = data.split('_')
            col = int(parts[2])
            row = int(parts[3])
            if row != game['current_level']:
                return

            monkey_col = game['monkey_positions'][row]
            extra_monkey_cols = game['extra_monkeys'][row]
            game['revealed'][row] = col

            if col == monkey_col or col in extra_monkey_cols:
                game['game_over'] = True
                game['state'] = 'ended'
                text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\nYou found the monkey and lost."
                game['ended_text'] = text
                keyboard = generate_grid_buttons(game, reveal_all=True) + get_persistent_buttons(game)
            else:
                game['current_level'] += 1
                if game['current_level'] == 9:
                    multiplier = MULTIPLIERS[game['chosen_mode']][8]
                    winnings = game['bet_amount'] * multiplier
                    update_user_balance(user_id, balance + winnings)
                    text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance + winnings:.2f}\n\nReached the top! Won ${winnings:.2f}"
                    game['state'] = 'ended'
                    game['game_over'] = True
                    game['ended_text'] = text
                    keyboard = generate_grid_buttons(game, reveal_all=True) + get_persistent_buttons(game)
                else:
                    potential_winnings = get_potential_winnings(game)
                    text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance:.2f}\n\nLevel {game['current_level'] + 1}: Choose a spot\nPotential Cash-Out: ${potential_winnings:.2f} USDT"
                    keyboard = generate_grid_buttons(game) + get_persistent_buttons(game)

            await context.bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))

        elif data == "tower_cash_out":
            if game['game_over'] or game['current_level'] == 0:
                return
            multiplier = MULTIPLIERS[game['chosen_mode']][game['current_level'] - 1]
            winnings = game['bet_amount'] * multiplier
            update_user_balance(user_id, balance + winnings)
            game['game_over'] = True
            game['state'] = 'ended'
            text = f"🐒 Monkey Tower\n\nBet: ${game['bet_amount']:.2f}\nBalance: ${balance + winnings:.2f}\n\nCashed out! Won ${winnings:.2f}"
            game['ended_text'] = text
            keyboard = generate_grid_buttons(game, reveal_all=True) + get_persistent_buttons(game)
            await context.bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard))
