import time
import torch
from peft import AutoPeftModelForCausalLM
from transformers import GenerationConfig
from transformers import AutoTokenizer

def process_data_sample(example):
    processed_example = "<|system|>\n You are a support chatbot who helps with user queries chatbot who always responds in the style of a professional.\n<|user|>\n" + example["instruction"] + "\n<|assistant|>\n"
    return processed_example


if __name__ == "__main__":
        
    inp_str = process_data_sample(
        {
            "instruction": "i have a question about cancelling order {{Order Number}}",
        }
    )
    
    tokenizer = AutoTokenizer.from_pretrained("/content/zephyr-support-chatbot")
    inputs = tokenizer(inp_str, return_tensors="pt").to("cuda")

    model = AutoPeftModelForCausalLM.from_pretrained(
        "/content/zephyr-support-chatbot",
        low_cpu_mem_usage=True,
        return_dict=True,
        torch_dtype=torch.float16,
        device_map="cuda")

    generation_config = GenerationConfig(
        do_sample=True,
        top_k=1,
        temperature=0.1,
        max_new_tokens=256,
        pad_token_id=tokenizer.eos_token_id
    )

    st_time = time.time()
    outputs = model.generate(**inputs, generation_config=generation_config)
    print(tokenizer.decode(outputs[0], skip_special_tokens=True))
    print(time.time()-st_time)