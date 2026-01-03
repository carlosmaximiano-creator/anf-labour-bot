import os
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👷‍♂️ ANF Labour Bot ativo!\n\nUse /start para testar."
    )

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN não definido")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    print("🤖 Bot iniciado com polling...")
    app.run_polling()

if __name__ == "__main__":
    main()
