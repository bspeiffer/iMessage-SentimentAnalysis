SELECT
    datetime (message.date / 1000000000 + strftime ("%s", "2001-01-01"), "unixepoch", "localtime") AS message_date,
    message.text,
    message.is_from_me,
    --message.is_emote, --no idea what this does yet
    --message.message_type_identifier, --ISO 8583 message format code
    message.associated_message_guid, --used for tapback as far as i know (using to remove rows)
    chat.chat_identifier,
    handle.id as handle_id
FROM
    chat
    JOIN chat_message_join ON chat. "ROWID" = chat_message_join.chat_id
    JOIN message ON chat_message_join.message_id = message. "ROWID"
    LEFT JOIN handle on message.handle_id = handle. "ROWID"
WHERE
    message.text like '%coppa%'
    AND message.is_from_me = 1
ORDER BY
    message_date ASC;


-- Count of All Chats by Identifier and Handle
SELECT
    chat.chat_identifier,
    handle.id,
    count(chat.chat_identifier) AS message_count
FROM
    chat
    JOIN chat_message_join ON chat. "ROWID" = chat_message_join.chat_id
    JOIN message ON chat_message_join.message_id = message. "ROWID"
    LEFT JOIN handle on message.handle_id = handle. "ROWID"
GROUP BY
    chat.chat_identifier, handle.id
ORDER BY
    message_count ASC;
