from celery import Celery
from connectors.os_query import insert_into_os
from transformers import AutoModelForCausalLM, AutoTokenizer

model_id = 'imTheGodfather/OpsHarmonySentinel_7B_alpha_GPTQ8'
model = AutoModelForCausalLM.from_pretrained(model_id, device_map="auto")
tokenizer = AutoTokenizer.from_pretrained(model_id, padding_side='left')

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task
def process_data(data):
    question = f"Analyse and provide root cause for the following ITOps incident. Use the forensics to support your answer. You should be technically accurate and up to the point. {data}"
    messages = [
    {"role": "system", "content": "You are a friendly chatbot assistant made by HEAL. You are an expert in ITOps, servers, applications and all other related domains. You answer specifically only to the question asked."},
    {"role": "user", "content": question},]
    gen_input = tokenizer.apply_chat_template(messages, return_tensors="pt").to('cuda')

    gen_output = model.generate(input_ids=gen_input,
                                max_new_tokens=512,
                                do_sample=True,
                                temperature=0.5,
                                top_k=50,
                                top_p=0.95,
                                repetition_penalty=1.1)
    out = tokenizer.decode(gen_output[0], skip_special_tokens=True)
    insert_into_os(df)
    print(out)

if __name__ == '__main__':
    app.worker_main(['worker', '--loglevel=info'])
