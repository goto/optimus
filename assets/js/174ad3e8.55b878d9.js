"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2389],{3905:(e,t,a)=>{a.d(t,{Zo:()=>d,kt:()=>h});var n=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},d=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},c="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),c=p(a),m=o,h=c["".concat(s,".").concat(m)]||c[m]||u[m]||r;return a?n.createElement(h,i(i({ref:t},d),{},{components:a})):n.createElement(h,i({ref:t},d))}));function h(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,i=new Array(r);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[c]="string"==typeof e?e:o,i[1]=l;for(var p=2;p<r;p++)i[p]=a[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},5105:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>u,frontMatter:()=>r,metadata:()=>l,toc:()=>p});var n=a(7462),o=(a(7294),a(3905));const r={},i="Create Job Specifications",l={unversionedId:"client-guide/create-job-specifications",id:"client-guide/create-job-specifications",title:"Create Job Specifications",description:"A Job is the fundamental execution unit of an Optimus data pipeline. It can be scheduled, configured and is always",source:"@site/docs/client-guide/create-job-specifications.md",sourceDirName:"client-guide",slug:"/client-guide/create-job-specifications",permalink:"/optimus/docs/client-guide/create-job-specifications",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/create-job-specifications.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1682651997,formattedLastUpdatedAt:"Apr 28, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Manage BigQuery Resource",permalink:"/optimus/docs/client-guide/manage-bigquery-resource"},next:{title:"Setting up Alert to Job",permalink:"/optimus/docs/client-guide/setting-up-alert"}},s={},p=[{value:"Initialize Job Specification",id:"initialize-job-specification",level:2},{value:"Understanding the Job Specifications",id:"understanding-the-job-specifications",level:2},{value:"Behavior",id:"behavior",level:3},{value:"Task",id:"task",level:3},{value:"Dependencies",id:"dependencies",level:3},{value:"Metadata",id:"metadata",level:3},{value:"Completing the Transformation Task",id:"completing-the-transformation-task",level:2},{value:"Adding Hook",id:"adding-hook",level:3}],d={toc:p},c="wrapper";function u(e){let{components:t,...r}=e;return(0,o.kt)(c,(0,n.Z)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"create-job-specifications"},"Create Job Specifications"),(0,o.kt)("p",null,"A Job is the fundamental execution unit of an Optimus data pipeline. It can be scheduled, configured and is always\nmapped to a single transformation type (eg, BQ-to-BQ, GCS-to-BQ, etc). It can have dependencies over other jobs and\nshould only execute once the dependent job is successfully completed."),(0,o.kt)("p",null,"A job can also be configured with Hooks as part of its lifecycle, which can be triggered before or after the job.\nPlease go through the ",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/job"},"concept")," to know more about it."),(0,o.kt)("p",null,"Before we begin, let\u2019s understand the flow of job creation & deployment (later) in Optimus."),(0,o.kt)("p",null,(0,o.kt)("img",{alt:"Create Job Flow",src:a(5591).Z,title:"CreateJobSpecFlow",width:"1600",height:"979"})),(0,o.kt)("p",null,'For this guide, we\'ll be creating a job that writes "hello YYYY-MM-DD" to a table every day at 03.00 AM. We\'ll use the\nBQ-to-BQ transformation type. For the purpose of this guide, we\'ll assume that the Google Cloud Project name is\n"sample-project" & dataset is just called "playground".'),(0,o.kt)("h2",{id:"initialize-job-specification"},"Initialize Job Specification"),(0,o.kt)("p",null,"Open your terminal and create a new directory that will hold the specifications created by Optimus CLI. Once ready,\nyou can run the following command and answer the corresponding prompts (do note that some prompts would be to select\nfrom options instead of input):"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job create       \n? Please choose the namespace: sample_namespace\n? Provide new directory name to create for this spec? [.] sample-project.playground.table1\n? What is the job name? sample-project.playground.table1\n? Who is the owner of this job? sample_owner\n? Select task to run? bq2bq\n? Specify the schedule start date 2023-01-26\n? Specify the schedule interval (in crontab notation) 0 2 * * *\n? Transformation window daily\n? Project ID sample-project\n? Dataset Name playground\n? Table ID table1\n? Load method to use on destination REPLACE\nJob successfully created at sample-project.playground.table1\n")),(0,o.kt)("p",null,"After running the job create command, the job specification file and assets directory are created in the following directory."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"\u251c\u2500\u2500 sample_namespace\n\u2502   \u2514\u2500\u2500 jobs\n|       \u2514\u2500\u2500 sample-project.playground.table1\n|           \u2514\u2500\u2500 assets\n|               \u2514\u2500\u2500 query.sql\n|           \u2514\u2500\u2500 job.yaml\n\u2502   \u2514\u2500\u2500 resources\n\u2514\u2500\u2500 optimus.yaml\n")),(0,o.kt)("p",null,"Do notice that query.sql file is also generated. This is because, for BQ to BQ job, transformation logic lies in the\nquery.sql file. We will update this file based on the requirement later."),(0,o.kt)("p",null,"For now, let\u2019s take a deeper look at the job.yaml that Optimus has generated and understands what it does. After\ntaking a look at the possible configurations, we will try to complete the transformation task and take a look at how\nto add a hook."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.playground.table1\nowner: sample_owner\nschedule:\n  start_date: "2023-01-26"\n  interval: 0 2 * * *\nbehavior:\n  depends_on_past: false\n  catch_up: false\ntask:\n  name: bq2bq\n  config:\n    DATASET: playground\n    LOAD_METHOD: REPLACE\n    PROJECT: sample-project\n    SQL_TYPE: STANDARD\n    TABLE: table1\nwindow:\n  size: 24h\n  offset: "0"\n  truncate_to: d\nlabels:\n  orchestrator: optimus\nhooks: []\ndependencies: []\n')),(0,o.kt)("h2",{id:"understanding-the-job-specifications"},"Understanding the Job Specifications"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Job Configuration"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Version"),(0,o.kt)("td",{parentName:"tr",align:null},"Version 1 and 2 (recommended) are available. This affects the windowing capability.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Name"),(0,o.kt)("td",{parentName:"tr",align:null},"Should be unique in the project.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Owner"),(0,o.kt)("td",{parentName:"tr",align:null},"Owner of the job, can be an email, team name, slack handle, or anything that works for your team.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Schedule"),(0,o.kt)("td",{parentName:"tr",align:null},"Specifications needed to schedule a job, such as start_date, end_date and interval (cron)")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Behavior"),(0,o.kt)("td",{parentName:"tr",align:null},"Specifications that represents how the scheduled jobs should behave, for example when the run is failed.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Task"),(0,o.kt)("td",{parentName:"tr",align:null},"Specifications related to the transformation task")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Hooks"),(0,o.kt)("td",{parentName:"tr",align:null},"Name & configuration of pre/post hooks. Take a look at how to add hooks ",(0,o.kt)("a",{parentName:"td",href:"#adding-hook"},"here"),".")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Labels"),(0,o.kt)("td",{parentName:"tr",align:null},"Help you to identify your job. Any of the values will also be marked as a tag in Airflow.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Dependencies"),(0,o.kt)("td",{parentName:"tr",align:null},"Represent the list of jobs that are considered upstream.")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Metadata"),(0,o.kt)("td",{parentName:"tr",align:null},"Represents additional resource and scheduler configurations.")))),(0,o.kt)("h3",{id:"behavior"},"Behavior"),(0,o.kt)("p",null,"Behavior specification might consist:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"depends_on_past: set to true to not allow the task to run, if the previous task run has not been succeeded yet"),(0,o.kt)("li",{parentName:"ul"},"catch_up: if the start date is in the past and catch_up is set to false, then the scheduler will not schedule the job since the start date."),(0,o.kt)("li",{parentName:"ul"},"retry",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"count: represents how many times it will try to retrigger the job if the job failed to run "),(0,o.kt)("li",{parentName:"ul"},"delay"),(0,o.kt)("li",{parentName:"ul"},"exponential_backoff"))),(0,o.kt)("li",{parentName:"ul"},"notify: Alert configurations. Take a look more at this ",(0,o.kt)("a",{parentName:"li",href:"/optimus/docs/client-guide/setting-up-alert"},"here"),".")),(0,o.kt)("h3",{id:"task"},"Task"),(0,o.kt)("p",null,"Task specification might consist:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"name"),(0,o.kt)("li",{parentName:"ul"},"config: Some configs might be needed for a specific task type. For example, for BQ to BQ task, it is required to have\nBQ_SERVICE_ACCOUNT, PROJECT, DATASET, TABLE, SQL_TYPE, LOAD_METHOD configs. Take a look at the details of what is load method here."),(0,o.kt)("li",{parentName:"ul"},"window: Take a look at the details of the window ",(0,o.kt)("a",{parentName:"li",href:"/optimus/docs/concepts/intervals-and-windows"},"here"),".")),(0,o.kt)("h3",{id:"dependencies"},"Dependencies"),(0,o.kt)("p",null,"Represent the list of jobs that are considered upstream."),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"If the job is in a different project, include the Optimus\u2019 project name in the prefix."),(0,o.kt)("li",{parentName:"ul"},"If the job is in the same project, simply mentioning the job name is sufficient.")),(0,o.kt)("p",null,"Example:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},"dependencies:\n- job: sample-project.playground.table1\n- job: other-project/other-project.playground.table2\n")),(0,o.kt)("h3",{id:"metadata"},"Metadata"),(0,o.kt)("p",null,"Below specifications can be set in Metadata section:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("strong",{parentName:"li"},"resource"),": set up CPU/memory request/limit"),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("strong",{parentName:"li"},"airflow"),": set up which Airflow pool and what is the queue configuration for this job")),(0,o.kt)("h2",{id:"completing-the-transformation-task"},"Completing the Transformation Task"),(0,o.kt)("p",null,"Let\u2019s retake a look at the generated task specifications"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},'task:\n  name: bq2bq\n  config:\n    DATASET: playground\n    LOAD_METHOD: REPLACE\n    PROJECT: sample-project\n    SQL_TYPE: STANDARD\n    TABLE: table1\nwindow:\n  size: 24h\n  offset: "0"\n  truncate_to: d\n')),(0,o.kt)("p",null,"Here are the details of each configuration and the allowed values:"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Config Name"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"),(0,o.kt)("th",{parentName:"tr",align:null},"Values"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"PROJECT"),(0,o.kt)("td",{parentName:"tr",align:null},"GCP project ID of the destination BigQuery table"),(0,o.kt)("td",{parentName:"tr",align:null})),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"DATASET"),(0,o.kt)("td",{parentName:"tr",align:null},"BigQuery dataset name of the destination table"),(0,o.kt)("td",{parentName:"tr",align:null})),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"TABLE"),(0,o.kt)("td",{parentName:"tr",align:null},"the table name of the destination table"),(0,o.kt)("td",{parentName:"tr",align:null})),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"LOAD_METHOD"),(0,o.kt)("td",{parentName:"tr",align:null},"method to load data to the destination tables"),(0,o.kt)("td",{parentName:"tr",align:null},"Take a detailed look ",(0,o.kt)("a",{parentName:"td",href:"https://github.com/goto/transformers/blob/main/task/bq2bq/README.md"},"here"))),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"PARTITION_FILTER"),(0,o.kt)("td",{parentName:"tr",align:null},"Used to identify target partitions to replace in a REPLACE query. This can be left empty and Optimus will figure out the target partitions automatically but it's cheaper and faster to specify the condition. This filter will be used as a where clause in a merge statement to delete the partitions from the destination table."),(0,o.kt)("td",{parentName:"tr",align:null},'event_timestamp >= "{{.DSTART}}" AND event_timestamp < "{{.DEND}}"')))),(0,o.kt)("p",null,"Now let's try to modify the core transformation logic that lies in ",(0,o.kt)("inlineCode",{parentName:"p"},"assets/query.sql"),'. Remember that we are going to\ncreate a job that writes "hello YYYY-MM-DD" to a table every day at 03.00 AM. Now, let\u2019s modify the query so it prints\nwhat we intended:'),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-sql"},'SELECT CONCAT("Hello, ", "{{.DEND}}") AS message;\n')),(0,o.kt)("p",null,"{{.DEND}} is a macro that is replaced with the current execution date (in YYYY-MM-DD format) of the task (\nnote that this is the execution date of when the task was supposed to run, not when it actually runs). Take a detailed\nlook at the supported macros ",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/macros"},"here"),"."),(0,o.kt)("p",null,"Do notice that the query is not sourcing from any other table. This means the job we are creating will not have any\n",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/dependency"},"dependency")," unless we manually specify so in the job specification YAML file.\nHowever, if for any reason you are querying from another resource and want to ignore the dependency, add\n",(0,o.kt)("inlineCode",{parentName:"p"},"@ignoreupstream")," annotation just before the table name, for example:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-sql"},"SELECT column1, column2, column3\nFROM `sample-project.playground.source1` s1\nLEFT JOIN /* @ignoreupstream */\n`sample-project.playground.source2` s2\nON (s1.id = s2.s1_id)\nWHERE\nDATE(`load_timestamp`) >= DATE('{{.DSTART}}')\nAND DATE(`load_timestamp`) < DATE('{{.DEND}}');\n")),(0,o.kt)("h3",{id:"adding-hook"},"Adding Hook"),(0,o.kt)("p",null,"There might be a certain operation that you might want to run before or after the Job. Please go through the\n",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/job"},"concept")," to know more about it."),(0,o.kt)("p",null,"For this guide, let\u2019s add a post hook that will audit our BigQuery data using ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/goto/predator"},"Predator"),".\nYou can find the Predator plugin YAML file ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/goto/predator/blob/main/optimus-plugin-predator.yaml"},"here"),"\nand have the plugin installed in your ",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/server-guide/installing-plugins"},"server")," and ",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/client-guide/installing-plugin"},"client"),"."),(0,o.kt)("p",null,"In order to add a hook to an existing Job, run the following command and answer the corresponding prompts:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},'$ optimus job addhook\n? Please choose the namespace: sample_namespace\n? Select a Job sample-project.playground.table1\n? Filter expression for extracting transformation rows? __PARTITION__ >= date("{{ .DSTART | Date }}") AND __PARTITION__ < date("{{ .DEND | Date }}")\n? Specify the profile/audit result grouping field (empty to not group the result)\n? Choose the profiling mode complete\n\nHook successfully added to sample-project.playground.table1\n')),(0,o.kt)("p",null,"With the above prompt, we're adding the predator hook post the execution of the primary job. Filter expression\nconfiguration and the rest of the questions are specific to a predator hook, and it might be different for other hooks."),(0,o.kt)("p",null,"After this, existing job.yaml file will get updated with the new hook config."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},"hooks:\n- name: predator\n  config:\n    AUDIT_TIME: '{{.EXECUTION_TIME}}'\n    BQ_DATASET: '{{.TASK__DATASET}}'\n    BQ_PROJECT: '{{.TASK__PROJECT}}'\n    BQ_TABLE: '{{.TASK__TABLE}}'\n    FILTER: __PARTITION__ >= date(\"{{ .DSTART | Date }}\") AND __PARTITION__ < date(\"{{ .DEND | Date }}\")\n    GROUP: \"\"\n    MODE: complete\n    PREDATOR_URL: '{{.GLOBAL__PREDATOR_HOST}}'\n    SUB_COMMAND: profile_audit\n")))}u.isMDXComponent=!0},5591:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/images/CreateJobSpecFlow-8a5b023871779f63e58da0ab2455770f.png"}}]);