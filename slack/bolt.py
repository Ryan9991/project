import slack
import os, json
from pathlib import Path
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.workflows.step import WorkflowStep

env_path = Path('.') / '.env'
load_dotenv(dotenv_path = env_path)

app = App(
    token='xoxb-2535785361-4078467525350-WVXdZ0AHuyDnXAgBL84qBGl9', 
    signing_secret='7e18eb143d0541436b138c5df2dfcbcf'
    )

def edit(ack, step, configure):
    ack()

    blocks = [
        {
            "type": "input",
            "block_id": "task_name_input",
            "element": {
                "type": "plain_text_input",
                "action_id": "name",
                "placeholder": {"type": "plain_text", "text": "Add a task name"},
            },
            "label": {"type": "plain_text", "text": "Task name"},
        },
        {
            "type": "input",
            "block_id": "task_description_input",
            "element": {
                "type": "plain_text_input",
                "action_id": "description",
                "placeholder": {"type": "plain_text", "text": "Add a task description"},
            },
            "label": {"type": "plain_text", "text": "Task description"},
        },
    ]
    configure(blocks=blocks)

def save(ack, view, update):
    ack()

    
    values = view["state"]["values"]
    print(values)
    task_name = values["task_name_input"]["name"]
    task_description = values["task_description_input"]["description"]
                
    inputs = {
        "task_name": {"value": task_name["value"]},
        "task_description": {"value": task_description["value"]}
    }
    outputs = [
        {
            "type": "text",
            "name": "task_name",
            "label": "Task name",
        },
        {
            "type": "text",
            "name": "task_description",
            "label": "Task description",
        }
    ]
    update(inputs=inputs, outputs=outputs)

def execute(step, complete, fail):
    inputs = step["inputs"]
    # if everything was successful
    outputs = {
        "task_name": inputs["task_name"]["value"],
        "task_description": inputs["task_description"]["value"],
    }
    complete(outputs=outputs)

    # if something went wrong
    error = {"message": "Just testing step failure!"}
    fail(error=error)

ws = WorkflowStep(
    callback_id="workflow_callback",
    edit=edit,
    save=save,
    execute=execute,
)

# Start your app
if __name__ == "__main__":
    app.step(ws)
    app.start(port=int(os.environ.get("PORT", 3000)))