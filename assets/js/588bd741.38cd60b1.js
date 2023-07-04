"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[4053],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>h});var n=a(7294);function s(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){s(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,s=function(e,t){if(null==e)return{};var a,n,s={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(s[a]=e[a]);return s}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(s[a]=e[a])}return s}var i=n.createContext({}),p=function(e){var t=n.useContext(i),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},c=function(e){var t=p(e.components);return n.createElement(i.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var a=e.components,s=e.mdxType,r=e.originalType,i=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),u=p(a),d=s,h=u["".concat(i,".").concat(d)]||u[d]||m[d]||r;return a?n.createElement(h,o(o({ref:t},c),{},{components:a})):n.createElement(h,o({ref:t},c))}));function h(e,t){var a=arguments,s=t&&t.mdxType;if("string"==typeof e||s){var r=a.length,o=new Array(r);o[0]=d;var l={};for(var i in t)hasOwnProperty.call(t,i)&&(l[i]=t[i]);l.originalType=e,l[u]="string"==typeof e?e:s,o[1]=l;for(var p=2;p<r;p++)o[p]=a[p];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}d.displayName="MDXCreateElement"},9386:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>i,contentTitle:()=>o,default:()=>m,frontMatter:()=>r,metadata:()=>l,toc:()=>p});var n=a(7462),s=(a(7294),a(3905));const r={},o="Quickstart",l={unversionedId:"getting-started/quick-start",id:"getting-started/quick-start",title:"Quickstart",description:"This quick start will guide you to try out Optimus fast without getting into many details. As part of this, you will",source:"@site/docs/getting-started/quick-start.md",sourceDirName:"getting-started",slug:"/getting-started/quick-start",permalink:"/optimus/docs/getting-started/quick-start",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/getting-started/quick-start.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1688450862,formattedLastUpdatedAt:"Jul 4, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Installation",permalink:"/optimus/docs/getting-started/installation"},next:{title:"Architecture",permalink:"/optimus/docs/concepts/architecture"}},i={},p=[{value:"Prerequisite",id:"prerequisite",level:2},{value:"Step 1: Start Server",id:"step-1-start-server",level:2},{value:"Start Server",id:"start-server",level:3},{value:"Step 2: Connect Client With Server",id:"step-2-connect-client-with-server",level:2},{value:"Step 3: Create BigQuery resource",id:"step-3-create-bigquery-resource",level:2},{value:"Step 4: Create &amp; Deploy Job",id:"step-4-create--deploy-job",level:2}],c={toc:p},u="wrapper";function m(e){let{components:t,...a}=e;return(0,s.kt)(u,(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,s.kt)("h1",{id:"quickstart"},"Quickstart"),(0,s.kt)("p",null,"This quick start will guide you to try out Optimus fast without getting into many details. As part of this, you will\nbe provided with step-by-step instructions to  start Optimus server, connect Optimus client with server, create\nBigQuery resource through Optimus, create BigQuery to BigQuery job, and deploy it."),(0,s.kt)("h2",{id:"prerequisite"},"Prerequisite"),(0,s.kt)("ul",null,(0,s.kt)("li",{parentName:"ul"},"Docker or a local installation of Optimus."),(0,s.kt)("li",{parentName:"ul"},(0,s.kt)("a",{parentName:"li",href:"https://www.postgresql.org/download/"},"Postgres")," database."),(0,s.kt)("li",{parentName:"ul"},"BigQuery project"),(0,s.kt)("li",{parentName:"ul"},(0,s.kt)("a",{parentName:"li",href:"https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html"},"Airflow"),(0,s.kt)("ul",{parentName:"li"},(0,s.kt)("li",{parentName:"ul"},"This is not mandatory to complete the quick start, but needed for scheduling jobs.")))),(0,s.kt)("h2",{id:"step-1-start-server"},"Step 1: Start Server"),(0,s.kt)("p",null,"Start server with GOTO\u2019s BigQuery to BigQuery ",(0,s.kt)("a",{parentName:"p",href:"https://github.com/goto/transformers"},"plugin"),"."),(0,s.kt)("p",null,"Create a config.yaml file:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-yaml"},"version: 1\n\nlog:\n  level: debug\n\nserve:\n  port: 9100\n  host: localhost\n  ingress_host: localhost:9100\n  app_key:\n  db:\n    dsn: postgres://<dbuser:dbpassword>@localhost:5432/dbname?sslmode=disable\n\nplugin:\n  artifacts:\n   - https://github.com/goto/transformers/releases/download/v0.2.1/transformers_0.2.1_macos_x86_64.tar.gz\n")),(0,s.kt)("p",null,(0,s.kt)("em",{parentName:"p"},"Note: make sure you put artifacts link that suitable to your system.")),(0,s.kt)("h3",{id:"start-server"},"Start Server"),(0,s.kt)("p",null,"With the config.yaml available in the same working directory, you can start server by running:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus serve --install-plugins\n")),(0,s.kt)("p",null,"This will automatically install the plugins as specified in your server configuration."),(0,s.kt)("h2",{id:"step-2-connect-client-with-server"},"Step 2: Connect Client With Server"),(0,s.kt)("p",null,"Go to the directory where you want to have your Optimus specifications. Create client configuration by using\noptimus ",(0,s.kt)("inlineCode",{parentName:"p"},"init")," command. An interactive questionnaire will be presented, such as below:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus init\n\n? What is the Optimus service host? localhost:9100\n? What is the Optimus project name? sample_project\n? What is the namespace name? sample_namespace\n? What is the type of data store for this namespace? bigquery\n? Do you want to add another namespace? No\nClient config is initialized successfully\n")),(0,s.kt)("p",null,"After running the init command, Optimus client config will be configured. Along with it, the directories for the chosen\nnamespaces, including the subdirectories for jobs and resources will be created with the following structure:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre"},"sample_project\n\u251c\u2500\u2500 sample_namespace\n\u2502   \u2514\u2500\u2500 jobs\n\u2502   \u2514\u2500\u2500 resources\n\u2514\u2500\u2500 optimus.yaml\n")),(0,s.kt)("p",null,"Below is the client configuration that has been generated:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nlog:\n  level: INFO\n  format: ""\nhost: localhost:9100\nproject:\n  name: sample_project\n  config: {}\nnamespaces:\n- name: sample_namespace\n  config: {}\n  job:\n    path: sample_namespace/jobs\n  datastore:\n    - type: bigquery\n      path: sample_namespace/resources\n      backup: {}\n')),(0,s.kt)("p",null,"Let\u2019s add ",(0,s.kt)("inlineCode",{parentName:"p"},"storage_path")," project configuration that is needed to store the result of job compilation and\n",(0,s.kt)("inlineCode",{parentName:"p"},"scheduler_host")," which is needed for compilation."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-yaml"},"project:\n  name: sample_project\n  config:\n    storage_path: file:///Users/sample_user/optimus/sample_project/compiled\n    scheduler_host: http://sample-host\n")),(0,s.kt)("p",null,(0,s.kt)("em",{parentName:"p"},"Note: storage path is the location where airflow is reading its dags from.")),(0,s.kt)("p",null,"Now, let's register ",(0,s.kt)("inlineCode",{parentName:"p"},"sample_project")," and ",(0,s.kt)("inlineCode",{parentName:"p"},"sample_namespace")," to your Optimus server."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus project register --with-namespaces\n")),(0,s.kt)("p",null,"You can verify if the project has been registered successfully by running this command:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus project describe\n")),(0,s.kt)("h2",{id:"step-3-create-bigquery-resource"},"Step 3: Create BigQuery resource"),(0,s.kt)("p",null,"Before creating BigQuery resources, make sure your Optimus server has access to your BQ project by adding a\n",(0,s.kt)("inlineCode",{parentName:"p"},"BQ_SERVICE_ACCOUNT")," secret."),(0,s.kt)("p",null,"Assume you have your service account json file in the same directory (project directory), create the secret using the\nfollowing command. Make sure the service account that you are using is authorized to create tables."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret set BQ_SERVICE_ACCOUNT --file service_account.json\n")),(0,s.kt)("p",null,"Check whether the secret has been registered successfully by running this command."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus secret list\n")),(0,s.kt)("p",null,"Now, let\u2019s create a resource using the following interactive command."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus resource create\n\n? Please choose the namespace: sample_namespace\n? What is the resource name? sample-project.sample_namespace.table1\n? What is the resource type? table\n? Provide new directory name to create for this spec? [sample_namespace/resources] sample-project.sample_namespace.table1\n\nResource spec [sample-project.sample_namespace.table1] is created successfully\n")),(0,s.kt)("p",null,(0,s.kt)("em",{parentName:"p"},"Note: resource name should be unique within the project. Take a look at the complete guide on how to create resource\n",(0,s.kt)("a",{parentName:"em",href:"/optimus/docs/client-guide/manage-bigquery-resource"},"here")," if needed.")),(0,s.kt)("p",null,"After running the command, the resource specification file will be automatically created in the following directory:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre"},"sample_project\n\u251c\u2500\u2500 sample_namespace\n\u2502   \u2514\u2500\u2500 jobs\n\u2502   \u2514\u2500\u2500 resources\n|       \u2514\u2500\u2500 sample-project.sample_namespace.table1\n|           \u2514\u2500\u2500 resource.yaml\n\u2514\u2500\u2500 optimus.yaml\n")),(0,s.kt)("p",null,"Let\u2019s open the resource.yaml file and add additional spec details as follows:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.sample_namespace.table1\ntype: table\nlabels: {}\nspec:\n  description: "sample optimus quick start table"\n  schema:\n  - name: sample_day\n    type: STRING\n    mode: NULLABLE\n  - name: sample_timestamp\n    type: TIMESTAMP\n    mode: NULLABLE\n')),(0,s.kt)("p",null,"Now that resource specification is complete, let\u2019s deploy this to the Optimus server and it will create the resource\nin BigQuery."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus resource upload-all --verbose\n\n> Validating namespaces\nnamespace validation finished!\n\n> Uploading all resources for namespaces [sample_namespace]\n> Deploying bigquery resources for namespace [sample_namespace]\n> Receiving responses:\n[success] sample-project.sample_namespace.table1\nresources with namespace [sample_namespace] are deployed successfully\nfinished uploading resource specifications to server!\n")),(0,s.kt)("h2",{id:"step-4-create--deploy-job"},"Step 4: Create & Deploy Job"),(0,s.kt)("p",null,"Sync plugins to your local for optimus to provide an interactive UI to add jobs, this is a prerequisite before\ncreating any jobs."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus plugin sync\n")),(0,s.kt)("p",null,"Let\u2019s verify if the plugin has been synced properly by running below command."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus version\n")),(0,s.kt)("p",null,"You should find ",(0,s.kt)("inlineCode",{parentName:"p"},"bq2bq")," plugin in the list of discovered plugins."),(0,s.kt)("p",null,"To create a job, we need to provide a job specification. Let\u2019s create one using the interactive optimus job command."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job create       \n? Please choose the namespace: sample_namespace\n? Provide new directory name to create for this spec? [.] sample-project.sample_namespace.table1\n? What is the job name? sample-project.sample_namespace.table1\n? Who is the owner of this job? sample_owner\n? Select task to run? bq2bq\n? Specify the schedule start date 2023-01-26\n? Specify the schedule interval (in crontab notation) 0 2 * * *\n? Transformation window daily\n? Project ID sample-project\n? Dataset Name sample_namespace\n? Table ID table1\n? Load method to use on destination REPLACE\nJob successfully created at sample-project.sample_namespace.table1\n")),(0,s.kt)("p",null,(0,s.kt)("em",{parentName:"p"},"Note: take a look at the details of job creation ",(0,s.kt)("a",{parentName:"em",href:"/optimus/docs/client-guide/create-job-specifications"},"here"),".")),(0,s.kt)("p",null,"After running the job create command, the job specification file and assets directory are created in the following directory."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre"},"\u251c\u2500\u2500 sample_namespace\n\u2502   \u2514\u2500\u2500 jobs\n|       \u2514\u2500\u2500 sample-project.sample_namespace.table1\n|           \u2514\u2500\u2500 assets\n|               \u2514\u2500\u2500 query.sql\n|           \u2514\u2500\u2500 job.yaml\n\u2502   \u2514\u2500\u2500 resources\n|       \u2514\u2500\u2500 sample-project.sample_namespace.table1\n|           \u2514\u2500\u2500 resource.yaml\n\u2514\u2500\u2500 optimus.yaml\n")),(0,s.kt)("p",null,"For BQ2BQ job, the core transformation logic lies in ",(0,s.kt)("inlineCode",{parentName:"p"},"assets/query.sql"),". Let\u2019s modify the query to the following script:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-sql"},'SELECT\nFORMAT_DATE(\'%A\', CAST("{{ .DSTART }}" AS TIMESTAMP)) AS `sample_day`,\nCAST("{{ .DSTART }}" AS TIMESTAMP) AS `sample_timestamp`;\n')),(0,s.kt)("p",null,(0,s.kt)("em",{parentName:"p"},"Note: take a look at Optimus\u2019 supported macros ",(0,s.kt)("a",{parentName:"em",href:"/optimus/docs/concepts/macros"},"here"),".")),(0,s.kt)("p",null,"Let\u2019s also verify the generated job.yaml file."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample-project.sample_namespace.table1\nowner: sample_owner\nschedule:\n  start_date: "2023-01-26"\n  interval: 0 2 * * *\nbehavior:\n  depends_on_past: false\ntask:\n  name: bq2bq\n  config:\n    DATASET: sample_namespace\n    LOAD_METHOD: REPLACE\n    PROJECT: sample-project\n    SQL_TYPE: STANDARD\n    TABLE: table1\nwindow:\n  size: 24h\n  offset: "0"\n  truncate_to: d\nlabels:\n  orchestrator: optimus\nhooks: []\ndependencies: []\n')),(0,s.kt)("p",null,"For this quick start, we are not adding any hooks, dependencies, or alert configurations. Take a look at the details\nof job specification and the possible options ",(0,s.kt)("a",{parentName:"p",href:"/optimus/docs/client-guide/create-job-specifications#understanding-the-job-specifications"},"here"),"."),(0,s.kt)("p",null,"Before proceeding, let\u2019s add the BQ_SERVICE_ACCOUNT secret in the task configuration."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-yaml"},'task:\n  name: bq2bq\n  config:\n    BQ_SERVICE_ACCOUNT: "{{.secret.BQ_SERVICE_ACCOUNT}}"\n    DATASET: sample_namespace\n...\n')),(0,s.kt)("p",null,"Later, you can avoid having the secret specified in every single job specification by adding it in the parent yaml\nspecification instead. For more details, you can take a look ",(0,s.kt)("a",{parentName:"p",href:"/optimus/docs/client-guide/organizing-specifications"},"here"),"."),(0,s.kt)("p",null,"Now the job specification has been prepared, lets try to add it to the server by running this command:"),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job replace-all --verbose\n\n> Validating namespaces\nvalidation finished!\n\n> Replacing all jobs for namespaces [sample_namespace]\n> Receiving responses:\n[sample_namespace] received 1 job specs\n[sample_namespace] found 1 new, 0 modified, and 0 deleted job specs\n[sample_namespace] processing job job1\n[sample_namespace] successfully added 1 jobs\nreplace all job specifications finished!\n")),(0,s.kt)("p",null,"Above command will try to add/modify all job specifications found in your project. We are not providing registering\na single job through Optimus CLI, but it is possible to do so using API."),(0,s.kt)("p",null,"Now that the jobs has been registered to Optimus, let\u2019s compile and upload it to the scheduler by using the following command."),(0,s.kt)("pre",null,(0,s.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus scheduler upload-all\n")),(0,s.kt)("p",null,"The command will try to compile your job specification to the DAG file. The result will be stored in the ",(0,s.kt)("inlineCode",{parentName:"p"},"storage_path"),"\nlocation as you have specified when configuring the optimus.yaml file."),(0,s.kt)("p",null,"Later, once you have Airflow ready and want to try out, this directory can be used as a source to be scheduled by Airflow."))}m.isMDXComponent=!0}}]);