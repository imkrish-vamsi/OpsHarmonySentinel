from langchain.chains import LLMChain
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv

load_dotenv()

def synthetic_root_cause_generator(sample_incident: str, sample_root_cause: str, output_template: str):
    gen_llm = ChatOpenAI(model='gpt-4-1106-preview', temperature=0.7)
    gen_prompt = PromptTemplate(input_variables=['sample_incident', 'sample_root_cause', 'output_template'], 
                                template="""You are an expert in ITSM domain. I need your help to create high quality 
                                dataset of root causes to fine tune a model much smaller than you to predict a root cause 
                                when an incident is given. Each incident is basically a problem along with a set of events 
                                which could have caused this problem. Here is a sample incident- {sample_incident} and its 
                                root cause- {sample_root_cause}. You should generate high quality and detailed root causes 
                                like the one provided to you, using various components known to you (like rhel, solaris, 
                                linux, mysql, percona, opensearch, redis, weblogic etc) and the problems that could occur 
                                in them. The output should be produced in this format- {output_template}. Remember 
                                that the root cause should be meaningful and related to a specific problem. You need not 
                                stick to the components in the example given. Its only for your reference. Generate 10 
                                samples to start with.""")
    
    gen_samples = LLMChain(llm=gen_llm, prompt=gen_prompt)
    samples = gen_samples.predict(sample_incident=sample_incident, sample_root_cause=sample_root_cause, output_template=output_template)

    return samples

if __name__ == "__main__":
    sample_incident = """[
        {
            "incidentId": "E-12-1-635-1700153820",
            "incidentStartTime": "11/16/2023 16:57",
            "probabilityScore": 0.82,
            "anomalyId": "AE-12-6224-1-C-S-ALL-28335897",
            "anomalyTimestamp": "11/16/2023 16:57",
            "applicationId": "shopping-cart-app",
            "instanceId": "mysql-percona-153",
            "serviceId": "mysql-percona-svc",
            "kpi": "CPU_UTIL",
            "value": 98.44,
            "thresholds": {"Upper": 0.0, "Lower": 85.0},
            "violationType": "Greater Than",
            "tags": [
            {"kpi": "CPU"},
            {"kpiCategory": "CPU"},
            {"kpiType:": "Core"},
            {"anomalyLevel": "INSTANCE"},
            {"severity": "SEVERE"}
            ],
            "components": [
            {"operatingSystem": "RHEL_8"},
            {"componentName": "MySql"},
            {"componentVersion": "8.0"}
            ],
        },
        {
            "anomalyId": "AE-12-6224-1-C-S-ALL-28335899",
            "anomalyTimestamp": "11/16/2023 16:59",
            "applicationId": "shopping-cart-app",
            "instanceId": "mysql-percona-153",
            "serviceId": "mysql-percona-svc",
            "kpi": "CPU_UTIL",
            "value": 99.2,
            "thresholds": {"Upper": 0.0, "Lower": 85.0},
            "violationType": "Greater Than",
            "tags": [
            {"kpi": "CPU"},
            {"kpiCategory": "CPU"},
            {"kpiType:": "Core"},
            {"anomalyLevel": "INSTANCE"},
            {"severity": "SEVERE"}
            ],
            "components": [
            {"operatingSystem": "RHEL_8"},
            {"componentName": "MySql"},
            {"componentVersion": "8.0"}
            ]
        },
        {
            "anomalyId": "AE-12-6224-6-C-S-ALL-28335898",
            "anomalyTimestamp": "11/16/2023 16:58",
            "applicationId": "shopping-cart-app",
            "instanceId": "mysql-percona-153",
            "serviceId": "mysql-percona-svc",
            "kpi": "LOAD_AVG",
            "value": 11.21,
            "thresholds": {"Upper": 0.0, "Lower": 8.0},
            "violationType": "Greater Than",
            "tags": [
            {"kpi": "LoadAvg"},
            {"kpiCategory": "CPU"},
            {"kpiType:": "Core"},
            {"anomalyLevel": "INSTANCE"},
            {"severity": "SEVERE"}
            ],
            "components": [
            {"operatingSystem": "RHEL_8"},
            {"componentName": "MySql"},
            {"componentVersion": "8.0"}
            ]
        },
        {
            "anomalyId": "AE-12-1031-397-T-S-28335897",
            "anomalyTimestamp": "11/16/2023 16:57",
            "applicationId": "shopping-cart-app",
            "serviceId": "shopping-product-service",
            "kpi": "RESPONSE_TIME",
            "value": 113.98,
            "thresholds": {"Upper": 0.0, "Lower": 100.0},
            "violationType": "Greater Than",
            "tags": [
            {"kpi": "RESPONSE TIME"},
            {"kpiCategory": "Workload"},
            {"kpiType:": "Core"},
            {"anomalyLevel": "CLUSTER"},
            {"severity": "SEVERE"}
            ],
            "components": [
            {"operatingSystem": "RHEL_8"},
            {"componentName": "Springboot Micro-service"},
            {"javaVersion": "8.0"}
            ]
        },
        {
            "anomalyId": "AE-12-1031-397-T-S-28335899",
            "anomalyTimestamp": "11/16/2023 16:57",
            "applicationId": "shopping-cart-app",
            "serviceId": "shopping-product-service",
            "kpi": "RESPONSE_TIME",
            "value": 117.92,
            "thresholds": {"Upper": 0.0, "Lower": 100.0},
            "violationType": "Greater Than",
            "tags": [
            {"kpi": "RESPONSE TIME"},
            {"kpiCategory": "Workload"},
            {"kpiType:": "Core"},
            {"anomalyLevel": "CLUSTER"},
            {"severity": "SEVERE"}
            ],
            "components": [
            {"operatingSystem": "RHEL_8"},
            {"componentName": "Springboot Micro-service"},
            {"javaVersion": "8.0"}
            ]
        }
    ]"""
    
    sample_root_cause = """Based on the provided information, the root cause of the incident could be high CPU utilization in the mysql-percona-153 instance of the 
    shopping-cart-app. This is indicated by the high CPU utilization values of 98.44 and 99.2, which exceed the upper threshold of 85.0. The anomalies 
    related to load average and response time may be secondary effects caused by the high CPU utilization."""

    output_template = """ JSON OBJECT WITH A SINGLE ITEM- 
    {   
        "root_causes" : ["root_cause1", "root_cause2", "root_cause3", ...]
    }
    and there should be no other text in the output because i am going to parse your output directly to a dataframe.
    """

    import json
    import pandas as pd
    
    json_str = synthetic_root_cause_generator(sample_incident, sample_root_cause, output_template)
    parsed_jstr = json_str.replace("```json", "").replace("```", "")
    df = pd.DataFrame(json.loads(parsed_jstr))
    df.to_csv('./data/root_causes_gpt_4_turbo.csv', header=False, index=False, mode='a')