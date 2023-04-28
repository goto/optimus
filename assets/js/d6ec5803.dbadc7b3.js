"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[7997],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>g});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=a.createContext({}),p=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=p(e.components);return a.createElement(s.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,r=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),c=p(n),m=i,g=c["".concat(s,".").concat(m)]||c[m]||d[m]||r;return n?a.createElement(g,o(o({ref:t},u),{},{components:n})):a.createElement(g,o({ref:t},u))}));function g(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=n.length,o=new Array(r);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[c]="string"==typeof e?e:i,o[1]=l;for(var p=2;p<r;p++)o[p]=n[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},6593:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>d,frontMatter:()=>r,metadata:()=>l,toc:()=>p});var a=n(7462),i=(n(7294),n(3905));const r={},o="Tutorial of Plugin Development",l={unversionedId:"building-plugin/tutorial",id:"building-plugin/tutorial",title:"Tutorial of Plugin Development",description:"To demonstrate the previous-mentioned wrapping functionality, let's create a plugin in Go as well as a yaml definition",source:"@site/docs/building-plugin/tutorial.md",sourceDirName:"building-plugin",slug:"/building-plugin/tutorial",permalink:"/optimus/docs/building-plugin/tutorial",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/building-plugin/tutorial.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1682651720,formattedLastUpdatedAt:"Apr 28, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Introduction of Plugin Development",permalink:"/optimus/docs/building-plugin/introduction"},next:{title:"Contributing",permalink:"/optimus/docs/contribute/contribution-process"}},s={},p=[{value:"Preparing task executor",id:"preparing-task-executor",level:2},{value:"Creating a yaml plugin",id:"creating-a-yaml-plugin",level:2},{value:"How to Use",id:"how-to-use",level:2},{value:"Installing the plugin in server",id:"installing-the-plugin-in-server",level:3},{value:"Installing the plugin in client",id:"installing-the-plugin-in-client",level:3},{value:"Use the plugin in job creation",id:"use-the-plugin-in-job-creation",level:3}],u={toc:p},c="wrapper";function d(e){let{components:t,...n}=e;return(0,i.kt)(c,(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"tutorial-of-plugin-development"},"Tutorial of Plugin Development"),(0,i.kt)("p",null,"To demonstrate the previous-mentioned wrapping functionality, let's create a plugin in Go as well as a yaml definition\nand use python for actual transformation logic. You can choose to fork this ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/kushsharma/optimus-plugins"},"example"),"\ntemplate and modify it as per your needs or start fresh. To demonstrate how to start from scratch, we will be starting\nfrom an empty git repository and build a plugin which will find potential hazardous ",(0,i.kt)("strong",{parentName:"p"},"Near Earth Orbit")," objects every\nday, let's call it ",(0,i.kt)("strong",{parentName:"p"},"neo")," for short."),(0,i.kt)("p",null,"Brief description of Neo is as follows"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Using NASA ",(0,i.kt)("a",{parentName:"li",href:"https://api.nasa.gov/"},"API")," we can get count of hazardous objects, their diameter and velocity."),(0,i.kt)("li",{parentName:"ul"},"Task will need two config as input, RANGE_START, RANGE_END as date time string which will filter the count for\nthis specific period only."),(0,i.kt)("li",{parentName:"ul"},"Execute every day, say at 2 AM."),(0,i.kt)("li",{parentName:"ul"},"Need a secret token that will be passed to NASA api endpoint for each request."),(0,i.kt)("li",{parentName:"ul"},"Output of this object count can be printed in logs for now but in a real use case can be pushed to Kafka topic or\nwritten to a database."),(0,i.kt)("li",{parentName:"ul"},"Plugin will be written in ",(0,i.kt)("strong",{parentName:"li"},"YAML")," format and Neo in ",(0,i.kt)("strong",{parentName:"li"},"python"),".")),(0,i.kt)("h2",{id:"preparing-task-executor"},"Preparing task executor"),(0,i.kt)("p",null,"Start by initialising an empty git repository with the following folder structure"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},".git\n/task\n  /neo\n    /executor\n      /main.py\n      /requirements.txt\n      /Dockerfile\nREADME.md\n")),(0,i.kt)("p",null,"That is three folders one inside another. This might look confusing for now, a lot of things will, but just keep going.\nCreate an empty python file in executor main.py, this is where the main logic for interacting with nasa api and\ngenerating output will be. For simplicity, lets use as minimal things as possible."),(0,i.kt)("p",null,"Add the following code to main.py"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'import os\nimport requests\nimport json\n\ndef start():\n    """\n    Sends a http call to nasa api, parses response and prints potential hazardous\n    objects in near earth orbit\n    :return:\n    """\n    opt_config = fetch_config_from_optimus()\n\n    # user configuration for date range\n    range_start = opt_config["envs"]["RANGE_START"]\n    range_end = opt_config["envs"]["RANGE_END"]\n\n    secret_key = os.environ["SECRET_KEY"]\n\n    # secret token required for NASA API being passed using job spec\n    api_key = json.loads(secret_key)\n    if api_key is None:\n        raise Exception("invalid api token")\n\n    # send the request for given date range\n    r = requests.get(url="https://api.nasa.gov/neo/rest/v1/feed",\n                     params={\'start_date\': range_start, \'end_date\': range_end, \'api_key\': api_key})\n\n    # extracting data in json format\n    print("for date range {} - {}".format(range_start, range_end))\n    print_details(r.json())\n\n    return\n  \n\ndef fetch_config_from_optimus():\n    """\n    Fetch configuration inputs required to run this task for a single schedule day\n    Configurations are fetched using optimus rest api\n    :return:\n    """\n    # try printing os env to see what all we have for debugging\n    # print(os.environ)\n\n    # prepare request\n    optimus_host = os.environ["OPTIMUS_HOSTNAME"]\n    scheduled_at = os.environ["SCHEDULED_AT"]\n    project_name = os.environ["PROJECT"]\n    job_name = os.environ["JOB_NAME"]\n\n    r = requests.post(url="http://{}/api/v1/project/{}/job/{}/instance".format(optimus_host, project_name, job_name),\n                      json={\'scheduled_at\': scheduled_at,\n                            \'instance_name\': "neo",\n                            \'instance_type\': "TASK"})\n    instance = r.json()\n\n    # print(instance)\n    return instance["context"]\n  \n \n  \nif __name__ == "__main__":\n    start()\n\n')),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"api_key")," is a token provided by nasa during registration. This token will be passed as a parameter in each http call.\n",(0,i.kt)("strong",{parentName:"p"},"SECRET_PATH")," is the path to a file which will contain this token in json and will be mounted inside the docker\ncontainer by Optimus."),(0,i.kt)("p",null,(0,i.kt)("strong",{parentName:"p"},"start")," function is using ",(0,i.kt)("strong",{parentName:"p"},"fetch_config_from_optimus()")," to get the date range for which this task executes for\nan iteration. In this example, configuration is fetched using REST APIs provided by optimus although there are variety\nof ways to get them. After extracting ",(0,i.kt)("strong",{parentName:"p"},"API_KEY")," from secret file, unmarshalling it to json with ",(0,i.kt)("strong",{parentName:"p"},"json.load()"),"\nsend a http request to nasa api. Response can be parsed and printed using the following function:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'def print_details(jd):\n    """\n    Parse and calculate what we need from NASA endpoint response\n\n    :param jd: json data fetched from NASA API\n    :return:\n    """\n    element_count = jd[\'element_count\']\n    potentially_hazardous = []\n    for search_date in jd[\'near_earth_objects\'].keys():\n        for neo in jd[\'near_earth_objects\'][search_date]:\n            if neo["is_potentially_hazardous_asteroid"] is True:\n                potentially_hazardous.append({\n                    "name": neo["name"],\n                    "estimated_diameter_km": neo["estimated_diameter"]["kilometers"]["estimated_diameter_max"],\n                    "relative_velocity_kmh": neo["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"]\n                })\n\n    print("total tracking: {}\\npotential hazardous: {}".format(element_count, len(potentially_hazardous)))\n    for haz in potentially_hazardous:\n        print("Name: {}\\nEstimated Diameter: {} km\\nRelative Velocity: {} km/h\\n\\n".format(\n            haz["name"],\n            haz["estimated_diameter_km"],\n            haz["relative_velocity_kmh"]\n        ))\n    return\n\n')),(0,i.kt)("p",null,"Finish it off by adding the main function"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'if __name__ == "__main__":\nstart()\n')),(0,i.kt)("p",null,"Add requests library in requirements.txt"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"requests==v2.25.1\n")),(0,i.kt)("p",null,"Once the python code is ready, wrap this in a Dockerfile"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-dockerfile"},'# set base image (host OS)\nFROM python:3.8\n\n# set the working directory in the container\nRUN mkdir -p /opt\nWORKDIR /opt\n\n# copy the content of the local src directory to the working directory\nCOPY task/neo/executor .\n\n# install dependencies\nRUN pip install -r requirements.txt\n\nCMD ["python3", "main.py"]\n\n')),(0,i.kt)("h2",{id:"creating-a-yaml-plugin"},"Creating a yaml plugin"),(0,i.kt)("p",null,"The Yaml implementation is as simple as providing the plugin details as below."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"name: Neo\ndescription: Near earth object tracker\nplugintype: task\npluginversion: latest\nimage: ghcr.io/kushsharma/optimus-task-neo-executor\nsecretpath: /tmp/.secrets\n\nquestions:\n- name: RANGE_START\n  prompt: Date range start\n  help: YYYY-MM-DD format\n  required: true\n- name: RANGE_END\n  prompt: Date range end\n  help: YYYY-MM-DD format\n  required: true\n")),(0,i.kt)("p",null,"Based on the usecase, additional validation can be added to the questions section."),(0,i.kt)("p",null,"This yaml plugin can be placed anywhere, however for this tutorial let\u2019s place it under ",(0,i.kt)("inlineCode",{parentName:"p"},"../task/neo"),"  directory and\nname it as ",(0,i.kt)("inlineCode",{parentName:"p"},"optimus-plugin-neo.yaml"),"."),(0,i.kt)("p",null,"Note: As part of this tutorial, we are not providing binary plugin tutorial as it is going to be deprecated. "),(0,i.kt)("h2",{id:"how-to-use"},"How to Use"),(0,i.kt)("p",null,"Before using, let\u2019s install this new plugin in server and client."),(0,i.kt)("h3",{id:"installing-the-plugin-in-server"},"Installing the plugin in server"),(0,i.kt)("p",null,"To use the created plugin in your server, you can simpy add the plugin path in the server config:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"plugin:\n  artifacts:\n   - ../task/neo/optimus-plugin-neo.yaml\n")),(0,i.kt)("p",null,"To apply the change, you can follow either of these options:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Start Optimus server using ",(0,i.kt)("inlineCode",{parentName:"li"},"--install-plugins")," flag, or"),(0,i.kt)("li",{parentName:"ul"},"Install the plugin separately before starting the server using ",(0,i.kt)("inlineCode",{parentName:"li"},"optimus plugin install")," command.")),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"Note: Take a look at installing plugins in server ",(0,i.kt)("a",{parentName:"em",href:"/optimus/docs/server-guide/installing-plugins"},"guide")," for more information.")),(0,i.kt)("h3",{id:"installing-the-plugin-in-client"},"Installing the plugin in client"),(0,i.kt)("p",null,"Install the plugin in client side by syncing it from server using below command."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus plugin sync\n")),(0,i.kt)("p",null,"Once finished, the ",(0,i.kt)("inlineCode",{parentName:"p"},"Neo")," plugin will be available in the ",(0,i.kt)("inlineCode",{parentName:"p"},".plugins")," directory."),(0,i.kt)("h3",{id:"use-the-plugin-in-job-creation"},"Use the plugin in job creation"),(0,i.kt)("p",null,"Once everything is built and in place, we can generate job specifications that uses neo as the task type."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"optimus create job\n? What is the job name? is_last_day_on_earth\n? Who is the owner of this job? owner@example.io\n? Which task to run? neo\n? Specify the start date 2022-01-25\n? Specify the interval (in crontab notation) 0 2 * * *\n? Transformation window daily\n? Date range start {{.DSTART}}\n? Date range end {{.DEND}}\njob created successfully is_last_day_on_earth\n")),(0,i.kt)("p",null,"Create a commit and deploy this specification if you are using optimus with a git managed repositories or send\na REST call or use GRPC, whatever floats your boat."),(0,i.kt)("p",null,"Once the job has been deployed and run, open the task log and verify something like this"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"total tracking: 14\npotential hazardous: 1\nName: (2014 KP4)\nEstimated Diameter: 0.8204270649 km\nRelative Velocity: 147052.9914506647 km/h\n")))}d.isMDXComponent=!0}}]);