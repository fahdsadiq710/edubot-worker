require('dotenv').config();
const { Telegraf } = require('telegraf');
const { createClient } = require('@supabase/supabase-js');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// ─────────────────────────────────────────────────────────────
// 1. VALIDATE ENVIRONMENT
// ─────────────────────────────────────────────────────────────
const REQUIRED_ENV = [
  'TELEGRAM_BOT_TOKEN',
  'SUPABASE_URL',
  'SUPABASE_SERVICE_ROLE_KEY',
  'GEMINI_API_KEY',
];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`❌  Missing env variable: ${key}`);
    process.exit(1);
  }
}

// ─────────────────────────────────────────────────────────────
// 2. INITIALIZE CLIENTS
// ─────────────────────────────────────────────────────────────
const bot = new Telegraf(process.env.TELEGRAM_BOT_TOKEN);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
);

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const geminiModel = genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });

// ─────────────────────────────────────────────────────────────
// 3. THE GEMINI SHIELD — RATE-LIMIT AWARE MESSAGE QUEUE
//
//    Free tier allows ~10–15 requests/min (≈1 req/6 s).
//    Each queue item waits QUEUE_DELAY_MS before the next
//    one begins, smoothing bursts into a controlled stream.
// ─────────────────────────────────────────────────────────────
const QUEUE_DELAY_MS = 5_000;   // 5 s between Gemini calls → safe at free tier
const MAX_QUEUE_SIZE = 50;      // drop oldest if flooded

/** @type {Array<{ ctx: import('telegraf').Context, telegramId: number, userMessage: string }>} */
const messageQueue = [];
let isProcessing   = false;

/**
 * Drain the queue one item at a time, respecting QUEUE_DELAY_MS between calls.
 */
async function processQueue() {
  if (isProcessing || messageQueue.length === 0) return;
  isProcessing = true;

  const { ctx, telegramId, userMessage } = messageQueue.shift();

  try {
    // ── Step A: Security check — fetch user from DB ──────────
    const { data: user, error: dbError } = await supabase
      .from('users')
      .select('id, full_name, goal, level, daily_time, is_active')
      .eq('telegram_id', telegramId)
      .single();

    // User not found or was deleted/banned by admin
    if (dbError || !user) {
      await ctx.reply(
        '⛔ Your account is not registered or has been suspended by the admin.\n' +
        'Please start from the website to create a new profile.'
      );
      isProcessing = false;
      setTimeout(processQueue, 1_000);
      return;
    }

    // Soft-ban: admin set is_active = false without deleting the row
    if (!user.is_active) {
      await ctx.reply('⛔ Your account has been deactivated. Contact support if this is a mistake.');
      isProcessing = false;
      setTimeout(processQueue, 1_000);
      return;
    }

    // ── Step B: Build dynamic prompt ────────────────────────
    const prompt = buildPrompt(user, userMessage);

    // ── Step C: Call Gemini ──────────────────────────────────
    const result       = await geminiModel.generateContent(prompt);
    const responseText = result.response.text();

    await ctx.reply(responseText, { parse_mode: 'Markdown' });

    // ── Step D: Log progress entry ───────────────────────────
    await supabase.from('progress').insert({
      user_id:    user.id,
      topic:      extractTopic(userMessage),
      evaluation: 'completed',
      mistakes:   [],          // TODO: parse from Gemini response in v2
      timestamp:  new Date().toISOString(),
    });

  } catch (err) {
    console.error('[Queue] Processing error:', err?.message ?? err);

    // Distinguish Gemini rate-limit errors (429) from other failures
    const isRateLimit = err?.message?.includes('429') || err?.status === 429;
    if (isRateLimit) {
      await ctx.reply(
        '⏳ Fox is a little busy right now (too many lessons at once!).\n' +
        'Your request is queued — try again in 30 seconds.'
      );
      // Re-queue the item at the front so it retries after back-off
      messageQueue.unshift({ ctx, telegramId, userMessage });
      isProcessing = false;
      setTimeout(processQueue, 30_000);   // back-off 30 s on rate limit
      return;
    }

    await ctx.reply('⚠️ A technical error occurred. Please try again in a moment.');
  }

  isProcessing = false;
  setTimeout(processQueue, QUEUE_DELAY_MS);
}

// ─────────────────────────────────────────────────────────────
// 4. PROMPT BUILDER
// ─────────────────────────────────────────────────────────────
function buildPrompt(user, userMessage) {
  const dailyMin = user.daily_time ?? 10;
  return `
You are Fox, a strict, sharp, and encouraging AI micro-learning coach.

**Student Profile:**
- Name: ${user.full_name}
- Learning Goal: ${user.goal}
- Current Level: ${user.level}
- Daily Study Budget: ${dailyMin} minutes

**Student's Message:**
"${userMessage}"

**Your Response Rules (follow exactly):**
1. Be concise — the student has only ${dailyMin} minutes today. No fluff.
2. If the student answered a question or exercise:
   a. State clearly whether they are CORRECT ✅ or INCORRECT ❌.
   b. If incorrect, pinpoint the exact mistake in ONE sentence.
   c. Provide the correct answer with a brief explanation (2–3 sentences max).
3. After correcting/confirming, immediately present the next challenge:
   - A short question, fill-in-the-blank, or real-world mini-scenario.
   - Tie it directly to their goal: "${user.goal}".
4. End with one motivational line. Keep it genuine, not generic.
5. Use simple Markdown (bold, bullet points) to make the response easy to read on Telegram.
6. Reply in the SAME LANGUAGE the student used in their message.
`.trim();
}

// ─────────────────────────────────────────────────────────────
// 5. HELPERS
// ─────────────────────────────────────────────────────────────

/** Naively extract a short topic label from the user message for progress logs. */
function extractTopic(message) {
  return message.slice(0, 80).trim();
}

/** Fetch user by telegram_id — returns null if not found or banned. */
async function getActiveUser(telegramId) {
  const { data, error } = await supabase
    .from('users')
    .select('id, full_name, goal, level, is_active')
    .eq('telegram_id', telegramId)
    .single();
  if (error || !data || !data.is_active) return null;
  return data;
}

// ─────────────────────────────────────────────────────────────
// 6. /start — DEEP LINKING (website → Telegram account claim)
//
//    Flow: user completes onboarding on site → site creates a
//    users row with a temp telegram_id and returns the UUID →
//    site redirects to t.me/BOT?start=UUID →
//    this handler updates the row with the real telegram_id
//    and activates the account.
// ─────────────────────────────────────────────────────────────
bot.start(async (ctx) => {
  const supabaseUserId = ctx.startPayload;   // UUID passed via deep link
  const telegramId     = ctx.from.id;        // real Telegram numeric ID
  const firstName      = ctx.from.first_name ?? 'learner';

  // Plain /start — no deep-link payload
  if (!supabaseUserId) {
    return ctx.reply(
      `👋 Welcome to *Fox AI*!\n\n` +
      `To start learning, visit our website and complete the 60-second onboarding.\n` +
      `You'll be sent directly back here with your personalised plan. 🚀`,
      { parse_mode: 'Markdown' }
    );
  }

  try {
    // Check if this Telegram ID is already linked to ANY account
    const existingUser = await getActiveUser(telegramId);
    if (existingUser) {
      return ctx.reply(
        `✅ Your account is already linked, *${existingUser.full_name}*!\n\n` +
        `Just send me a message to continue learning about *${existingUser.goal}*. 🎯`,
        { parse_mode: 'Markdown' }
      );
    }

    // Claim the pre-created onboarding row
    const { data, error } = await supabase
      .from('users')
      .update({
        telegram_id: telegramId,
        is_active:   true,
      })
      .eq('id', supabaseUserId)
      .select('full_name, goal, level, daily_time')
      .single();

    if (error || !data) {
      console.error('[/start] Link error:', error?.message);
      return ctx.reply(
        "❌ I couldn't find your onboarding profile. Please go back to the website and try again.\n" +
        'The link may have expired or already been used.'
      );
    }

    const levelEmoji = { beginner: '🌱', intermediate: '📚', advanced: '🚀' }[data.level] ?? '📖';

    await ctx.reply(
      `🎉 *Account linked successfully, ${data.full_name}!*\n\n` +
      `Here's your study plan:\n` +
      `• 🎯 Goal: *${data.goal}*\n` +
      `• ${levelEmoji} Level: *${data.level}*\n` +
      `• ⏱ Daily time: *${data.daily_time} minutes*\n\n` +
      `Send me any question or topic related to your goal and I'll start coaching you.\n` +
      `Type *"Start"* to get your very first lesson! 🦊`,
      { parse_mode: 'Markdown' }
    );

  } catch (err) {
    console.error('[/start] Unexpected error:', err?.message ?? err);
    ctx.reply('⚠️ An internal error occurred during account linking. Please try again.');
  }
});

// ─────────────────────────────────────────────────────────────
// 7. /progress — show the user their session history
// ─────────────────────────────────────────────────────────────
bot.command('progress', async (ctx) => {
  const telegramId = ctx.from.id;

  const user = await getActiveUser(telegramId);
  if (!user) {
    return ctx.reply('⛔ Account not found. Visit the website to register.');
  }

  const { data: sessions, error } = await supabase
    .from('progress')
    .select('topic, evaluation, timestamp')
    .eq('user_id', user.id)
    .order('timestamp', { ascending: false })
    .limit(10);

  if (error || !sessions?.length) {
    return ctx.reply("You haven't completed any sessions yet. Send me a message to start! 🚀");
  }

  const lines = sessions.map((s, i) => {
    const date = new Date(s.timestamp).toLocaleDateString('en-GB', {
      day: '2-digit', month: 'short',
    });
    return `${i + 1}. *${s.topic.slice(0, 40)}* — ${date}`;
  });

  await ctx.reply(
    `📊 *Your last ${sessions.length} sessions, ${user.full_name}:*\n\n` +
    lines.join('\n') +
    '\n\nKeep it up! Consistency is everything. 💪',
    { parse_mode: 'Markdown' }
  );
});

// ─────────────────────────────────────────────────────────────
// 8. /help — command reference
// ─────────────────────────────────────────────────────────────
bot.command('help', (ctx) => {
  ctx.reply(
    `🦊 *Fox AI — Commands*\n\n` +
    `/start — Link your website account\n` +
    `/progress — View your last 10 sessions\n` +
    `/help — Show this message\n\n` +
    `Or just *send any message* and Fox will coach you instantly.`,
    { parse_mode: 'Markdown' }
  );
});

// ─────────────────────────────────────────────────────────────
// 9. HANDLE INCOMING TEXT MESSAGES
// ─────────────────────────────────────────────────────────────
bot.on('text', async (ctx) => {
  const telegramId  = ctx.from.id;
  const userMessage = ctx.message.text.trim();

  // Guard: ignore empty messages
  if (!userMessage) return;

  // Guard: queue overflow protection
  if (messageQueue.length >= MAX_QUEUE_SIZE) {
    return ctx.reply(
      '⚠️ Fox is under heavy load right now. Please wait a moment and try again.'
    );
  }

  // Acknowledge receipt immediately (cheap — no Gemini call)
  const position = messageQueue.length + (isProcessing ? 1 : 0);
  const waitMsg  = position > 0
    ? `⏳ Analysing... (you are #${position + 1} in the queue)`
    : '⏳ Analysing your message...';

  await ctx.reply(waitMsg);

  // Push to queue
  messageQueue.push({ ctx, telegramId, userMessage });
  processQueue();
});

// ─────────────────────────────────────────────────────────────
// 10. GLOBAL ERROR HANDLERS
// ─────────────────────────────────────────────────────────────
bot.catch((err, ctx) => {
  console.error(`[Telegraf] Unhandled error for update ${ctx.updateType}:`, err?.message ?? err);
});

process.on('unhandledRejection', (reason) => {
  console.error('[Process] Unhandled rejection:', reason);
});

// ─────────────────────────────────────────────────────────────
// 11. LAUNCH — POLLING MODE (for Render / Railway)
//    Polling is reliable on free-tier hosts where webhook
//    HTTPS certificates and static IPs are unavailable.
// ─────────────────────────────────────────────────────────────
bot.launch({
  allowedUpdates: ['message', 'callback_query'],
  dropPendingUpdates: true,   // discard stale messages from while bot was offline
})
  .then(() => {
    console.log('🤖 Fox Telegram Bot Worker running in Polling mode...');
    console.log(`📋 Queue capacity: ${MAX_QUEUE_SIZE} messages`);
    console.log(`⏱  Gemini rate-limit delay: ${QUEUE_DELAY_MS / 1000}s between calls`);
  })
  .catch((err) => {
    console.error('❌ Failed to launch bot:', err?.message ?? err);
    process.exit(1);
  });

// Graceful shutdown
process.once('SIGINT',  () => { console.log('Shutting down (SIGINT)…');  bot.stop('SIGINT');  });
process.once('SIGTERM', () => { console.log('Shutting down (SIGTERM)…'); bot.stop('SIGTERM'); });
