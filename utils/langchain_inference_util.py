from dotenv import load_dotenv
from langchain.llms import OpenAI
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

# loads and stores variables into environment
load_dotenv()

def root_cause_investigator(incident_details_json: str):
    rc_llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0.7)
    prompt = PromptTemplate(
        input_variables=["incident"],
        template="""You are an expert in ITSM domain. Given is an incident which 
        comprises of events which could have caused it. You have to carefully go 
        through the input, investigate and provide only the root cause of what 
        could have caused this incident. Be precise, concise and upto the point. 
        You must openly acknowledge if you are not confident enough of what could 
        have caused the incident. Also note that the events are not in the actual 
        order in which the problem could have occurred. Its upto you to figure it 
        out. Here is the incident - {incident}"""
    )
    rc_chain = LLMChain(llm=rc_llm, prompt=prompt)
    analysis = rc_chain.predict(incident=incident_details_json)

    return analysis

def generate_forensic_scripts(incident:str, analysis: str, output_format: str):
    scripts_llm = ChatOpenAI(model_name="gpt-3.5-turbo-1106", temperature=0.5)
    prompt = PromptTemplate(
        input_variables=["incident", "analysis", "output_format"],
        template= """You are an expert in ITSM domain. Your expertise includes 
        investigating ITSM issues, identifying root causes, and resolving incidents. 
        I need your assistance with an incident that involves events and a potential 
        root cause. Evaluate the incident data and the provided root cause. If you 
        deem further investigation necessary to accurately pinpoin the cause of the 
        incident, generate accurate forensic scripts to be executed on relevant hosts. 
        The output should only be a single JSON object with one or more scripts with 
        keys indicating where to run each script, what information the script extracts 
        and values containing the corresponding accurate forensic script. If no further 
        investigation is needed, respond with "Required information is already available. 
        No need to run forensics". Here's the incident - {incident}, and the possible 
        root cause - {analysis}. Here is an example output format - {output_format} 
        """
    )
    script_gen_chain = LLMChain(llm=scripts_llm, prompt=prompt)
    scripts = script_gen_chain.predict(incident=incident, analysis=analysis, output_format=output_format)

    return scripts
 
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
    
    sample_forensic_output_format = """
    {
        "script1": {
        "host": "mysql-percona-153",
        "informationExtracted": "CPU utilization and processes",
        "script": "ps -aux"
    },
    "script2": {
        "host": "mysql-percona-153",
        "informationExtracted": "MySQL performance and status",
        "script": "mysqladmin extended-status"
        }
    }"""
    analysis = root_cause_investigator(sample_incident)
    print(f"Analysis: {analysis} \n")
    scripts = generate_forensic_scripts(sample_incident, analysis, sample_forensic_output_format)
    print(f"{scripts}")
