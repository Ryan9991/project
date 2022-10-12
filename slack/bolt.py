import os, json
from pathlib import Path
from dotenv import load_dotenv
from slack_bolt import App
from slack_sdk.errors import SlackApiError
import pandas as pd
import datetime
from load import load_to_bq

env_path = Path('.') / '.env'
load_dotenv(dotenv_path = env_path)


app = App(
    token=os.environ['SLACK_TOKEN'], 
    signing_secret=os.environ['SIGNING_SECRET']
)


# The open_modal shortcut listens to a shortcut with the callback_id "open_modal"
@app.shortcut("bug_report")
def open_modal(ack, shortcut, client):
    # Acknowledge the shortcut request
    ack()
    # Call the views_open method using the built-in WebClient
    with open('resources/modal.json') as f:
        client.views_open(
            trigger_id=shortcut["trigger_id"],
            # A simple view payload for a modal
            view = json.load(f)
        )


@app.view("submission_modal")
def handle_view_events(ack, body, client):
    print('submission')
    print(body)

    user                = body["user"]["id"]
    entity_id           = body["view"]["state"]["values"]["entity"]["entity"]["selected_option"]["text"]["text"]
    department_id       = body["view"]["state"]["values"]["department"]["department"]["selected_option"]["text"]["text"]
    brand_id            = body["view"]["state"]["values"]["brands_affected"]["brand"]["value"]
    summary_id          = body["view"]["state"]["values"]["summary_report"]["summary"]["value"]
    link_id             = body["view"]["state"]["values"]["dashboard_link"]["link"]["value"]
    note_id             = body["view"]["state"]["values"]["additional_note"]["note"]["value"]
    expected_due        = body["view"]["state"]["values"]["expected_due"]["due"]["value"]

    user_id             = f'*From*: <@{user}>'
    entity              = f'*Entity*: {entity_id}'
    department          = f'*Department*: {department_id}'
    brand               = f'*Brand*: {brand_id}'
    summary             = f'*Report*:\n {summary_id}'
    link                = f'*Dashboard link*:\n {link_id}'
    additional_note     = f'*Other*:\n {note_id}'
    due                 = f'*Expected done in {expected_due}*'
    enterprise_sector   = ["Enterprise (Commerce, ICUBE/Swift)", "Swiftanalytics / Share Dashboard"]
    entrepreneur_sector = ["Entrepreneur (Store, Orami)", "Holding (Finance, IR, Communication)"]

    if entity_id in enterprise_sector:
        responsible_team  = "<!subteam^S02T6PW3YER> or <@U03GSM3PELF> kindly review this report"
        # responsible_team  = "enter"
        team = "<!subteam^S02T6PW3YER>"
    elif entity_id in entrepreneur_sector:
        responsible_team  = "<!subteam^S02TC1305FE> or <@U03GSM3PELF> kindly review this report"
        # responsible_team  = "entre"
        team = "<!subteam^S02TC1305FE>"
    else:
        responsible_team  = "<!subteam^S03P4NQNS2U> or <@U03GSM3PELF> kindly review this report"
        # responsible_team  = "nre"
        team = "<!subteam^S03P4NQNS2U>"

    ack()

    value = [user_id, entity, department, brand, summary, link, additional_note, due, responsible_team]

    with open('resources/submission.json') as f:
        f = json.load(f)
        for i in range(0, 9):
            f[i+1]["text"]["text"] = value[i]

        result = client.chat_postMessage(
            channel = 'C040H22LV5J', 
            blocks  = f
            )

    ts = result['ts']
    print(ts)
    try:
        # Call the chat.postMessage method using the WebClient
        # The client passes the token you included in initialization    
        result = client.chat_postMessage(
            channel=user,
            text=f"Thanks for submitting your report! :siap:\n \nWe'll get back to you in <#C040H22LV5J> asap. \n \nImportant note: \n \n*Requesters don't have permission to approve a ticket, please notify data team members {team}*"
            # You could also use a blocks[] array to send richer content
        )

    except SlackApiError as e:
        print(f"Error: {e}")


@app.action("approve")
def handle_some_action(ack, body, client):
    print(body)
    ack()
    try:
        # Call the chat.postMessage method using the WebClient
        # The client passes the token you included in initialization    
        client.chat_postMessage(
            channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"],
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Hi {body['message']['blocks'][1]['text']['text'][8:]} :wave: Your Request is approved by <@{body['user']['id']}>, We'll Notify You When It's Done"
                    }
                }
            ]
            # You could also use a blocks[] array to send richer content
        )

    except SlackApiError as e:
        print(f"Error: {e}")

    try:
        with open('resources/in_progress.json') as f:
            client.chat_postMessage(
                channel     = body["channel"]["id"],
                thread_ts   = body["message"]["ts"],
                blocks      = json.load(f)
            )

    except SlackApiError as e:
        print(f"Error: {e}")

    try:
        del body['message']['blocks'][10]
        approved_blocks = [
                {
                "type": "section",
                "block_id": "approved_by",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Your request is Approved by <@{body['user']['id']}>",
                    "verbatim": False
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Status : Approved*"
                }
            }
        ]
        body['message']['blocks'].extend(approved_blocks)
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["message"]["ts"],
            blocks = body['message']['blocks']
        )
    except SlackApiError as e:
        print(f"Error: {e}")
        

@app.action("decline")
def handle_some_action(ack, body, client):
    ack()
    try:
        # Call the chat.postMessage method using the WebClient
        # The client passes the token you included in initialization    
        client.chat_postMessage(
            channel=body["channel"]["id"],
            thread_ts=body["message"]["ts"],
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Hi {body['message']['blocks'][1]['text']['text'][8:]} :wave: Your Request is declined by <@{body['user']['id']}>"
                    }
                }
            ]
            # You could also use a blocks[] array to send richer content
        )

    except SlackApiError as e:
        print(f"Error: {e}")

    try:
        del body['message']['blocks'][10]
        declined_blocks = [
                {
                "type": "section",
                "block_id": "declined_by",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Your request is Declined by <@{body['user']['id']}>",
                    "verbatim": False
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Status : Declined*"
                }
            }
        ]
        body['message']['blocks'].extend(declined_blocks)
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["message"]["ts"],
            blocks = body['message']['blocks']
        )
    except SlackApiError as e:
        print(f"Error: {e}")


@app.action("in_progress")
def handle_some_action(ack, body, client):
    print(body)
    ack()
    
    try:
        with open('resources/review.json') as f:
            client.chat_postMessage(
                channel     = body["channel"]["id"],
                thread_ts   = body["message"]["ts"],
                blocks      = json.load(f)
            )

    except SlackApiError as e:
        print(f"Error: {e}")

    
    try:
        result = client.conversations_replies(
            channel=body["channel"]["id"],
            inclusive=True,
            ts = body['container']['thread_ts']
        )

    except SlackApiError as e:
        print(f"Error: {e}")

    blocks = result['messages'][0]['blocks']

    try:
        blocks[13]['text']['text'] = '*Status : In Progress*'
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["container"]["thread_ts"],
            blocks = blocks
        )
    except SlackApiError as e:
        print(f"Error: {e}") 

    try:
        updated_body = body['message']['blocks'][0]
        print(updated_body)
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["container"]["message_ts"],
            blocks = [updated_body]
        )
    except SlackApiError as e:
        print(f"Error: {e}") 


@app.action("review")
def handle_some_action(ack, body, client):
    print(body)
    ack()

    try:
        with open('resources/done.json') as f:
            client.chat_postMessage(
                channel     = body["channel"]["id"],
                thread_ts   = body["message"]["ts"],
                blocks      = json.load(f)
            )

    except SlackApiError as e:
        print(f"Error: {e}") 

    try:
        result = client.conversations_replies(
            channel=body["channel"]["id"],
            inclusive=True,
            ts = body['container']['thread_ts']
        )

    except SlackApiError as e:
        print(f"Error: {e}")

    blocks = result['messages'][0]['blocks']

    try:
        blocks[13]['text']['text'] = '*Status : User Review*'
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["container"]["thread_ts"],
            blocks = blocks
        )
    except SlackApiError as e:
        print(f"Error: {e}") 

    user_id = result["messages"][0]["blocks"][1]["text"]["text"][10:-1]

    try:
        client.chat_postMessage(
                channel     = user_id,
                text        = f"Hi <@{user_id}>:wave:, Your request is solved, please recheck again with maximum of 3 workdays."
            )

    except SlackApiError as e:
        print(f"Error: {e}")

    try:
        updated_body = body['message']['blocks'][0]
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["container"]["message_ts"],
            blocks = [updated_body]
        )
    except SlackApiError as e:
        print(f"Error: {e}") 

    try:
        client.users.identity(

        )
    except SlackApiError as e:
        print(f"Error: {e}") 


@app.action('done')
def record(ack, body, client):
    ack()
    try:
    # Call the conversations.history method using the WebClient
    # The client passes the token you included in initialization    
        result = client.conversations_replies(
            channel=body["channel"]["id"],
            inclusive=True,
            ts = body['container']['thread_ts']
        )

        # Print message text
    except SlackApiError as e:
        print(f"Error: {e}")
    
    blocks = result['messages'][0]['blocks']

    try:
        blocks[13]['text']['text'] = '*Status : Done*'
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["container"]["thread_ts"],
            blocks = blocks
        )
    except SlackApiError as e:
        print(f"Error: {e}") 

    try:
        updated_body = body['message']['blocks'][0]
        client.chat_update(
            channel = body["channel"]["id"],
            ts = body["container"]["message_ts"],
            blocks = [updated_body]
        )
    except SlackApiError as e:
        print(f"Error: {e}") 

    done_ts = body["actions"][0]["action_ts"]

    load_to_bq(result, done_ts)


# Start your app
if __name__ == "__main__":
    app.start(port=int(os.environ.get("PORT", 5000)))
