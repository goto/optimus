"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2314],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>m});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},u=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},h=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),c=p(n),h=a,m=c["".concat(s,".").concat(h)]||c[h]||d[h]||o;return n?r.createElement(m,l(l({ref:t},u),{},{components:n})):r.createElement(m,l({ref:t},u))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,l=new Array(o);l[0]=h;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[c]="string"==typeof e?e:a,l[1]=i;for(var p=2;p<o;p++)l[p]=n[p];return r.createElement.apply(null,l)}return r.createElement.apply(null,n)}h.displayName="MDXCreateElement"},6034:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>d,frontMatter:()=>o,metadata:()=>i,toc:()=>p});var r=n(7462),a=(n(7294),n(3905));const o={},l=void 0,i={unversionedId:"rfcs/replay_rate_limiting",id:"rfcs/replay_rate_limiting",title:"replay_rate_limiting",description:"- Feature Name: Replay Rate Limit",source:"@site/docs/rfcs/20220525_replay_rate_limiting.md",sourceDirName:"rfcs",slug:"/rfcs/replay_rate_limiting",permalink:"/optimus/docs/rfcs/replay_rate_limiting",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/rfcs/20220525_replay_rate_limiting.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1730968315,formattedLastUpdatedAt:"Nov 7, 2024",sidebarPosition:20220525,frontMatter:{}},s={},p=[{value:"Background :",id:"background-",level:2},{value:"Approach :",id:"approach-",level:2},{value:"Reduce the load of Execution Project",id:"reduce-the-load-of-execution-project",level:3},{value:"Reduce the load of Airflow Default Pool",id:"reduce-the-load-of-airflow-default-pool",level:3}],u={toc:p},c="wrapper";function d(e){let{components:t,...n}=e;return(0,a.kt)(c,(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Feature Name: Replay Rate Limit"),(0,a.kt)("li",{parentName:"ul"},"Status: Draft"),(0,a.kt)("li",{parentName:"ul"},"Start Date: 2022-05-25"),(0,a.kt)("li",{parentName:"ul"},"Authors: Arinda")),(0,a.kt)("h1",{id:"summary"},"Summary"),(0,a.kt)("p",null,"Replay is using the same scheduler pool and executor slots as the scheduled jobs. Current Replay is predicted to be\ncausing issues and impacting the scheduled jobs, which might impacting the SLA of users. There should be a mechanism to\nuse a different pool and slots and configurable by the users so any backfilling will not impact and causing delay of\nthe scheduled jobs."),(0,a.kt)("h1",{id:"technical-design"},"Technical Design"),(0,a.kt)("h2",{id:"background-"},"Background :"),(0,a.kt)("p",null,"Currently, for bq2bq task the job is being executed using the project that specified in EXECUTION_PROJECT. This is also\napplied when the jobs are run through replay command. How Replay works is by clearing runs of the requested jobs and its\ndependency (if required), and there is no difference between the process of scheduled/manual job and replay. This also\nmeans the execution project and scheduler pool is also the same."),(0,a.kt)("h2",{id:"approach-"},"Approach :"),(0,a.kt)("h3",{id:"reduce-the-load-of-execution-project"},"Reduce the load of Execution Project"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"There will be ",(0,a.kt)("inlineCode",{parentName:"li"},"REPLAY_EXECUTION_PROJECT")," configuration, it can be set through the task config, project or namespace config."),(0,a.kt)("li",{parentName:"ul"},"When building instances, Optimus will check if there is a replay request for the particular job and date, if yes,\n",(0,a.kt)("inlineCode",{parentName:"li"},"EXECUTION_PROJECT")," will be replaced with ",(0,a.kt)("inlineCode",{parentName:"li"},"REPLAY_EXECUTION_PROJECT"),".")),(0,a.kt)("p",null,"In job specs, task level:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"task:\n  name: bq2bq\n  config:\n    PROJECT: sample-project\n    DATASET: sample_dataset\n    TABLE: sample_table\n    SQL_TYPE: STANDARD\n    LOAD_METHOD: REPLACE\n    EXECUTION_PROJECT: main-executor-project\n    REPLAY_EXECUTION_PROJECT: replay-executor-project\n")),(0,a.kt)("h3",{id:"reduce-the-load-of-airflow-default-pool"},"Reduce the load of Airflow Default Pool"),(0,a.kt)("p",null,"Initially, there are 3 mechanism to be considered for this."),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"Differentiating Airflow pool for Replay."),(0,a.kt)("li",{parentName:"ol"},"Trigger a new run for Replay (not using current clear method)."),(0,a.kt)("li",{parentName:"ol"},"Limit the slot through Optimus.")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"For the first option"),", Airflow pool is configured through DAG. This approach requires task to be configured using\neach of the expected pool, for example ",(0,a.kt)("inlineCode",{parentName:"p"},"bq2bq-default")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"bq2bq-replay"),". The task to be selected is being decided\nusing ",(0,a.kt)("inlineCode",{parentName:"p"},"BranchPythonOperator"),". This is not preferred as there will numerous task being set and visualize, and might\nconfuse users."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"For the second option"),", the replay run is being triggered using API, not by using the clear run method. However,\nAirflow is not accepting a pool configuration, thus it will still goes to the same pool."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"The third option"),", is limiting the slot through Optimus. There will be configuration in project level to set the\nnumber of slots Replay can occupy."),(0,a.kt)("p",null,"Project configuration"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"project:\n  name: sample-project\n  config:\n    storage_path: gs://sample-bucket\n    max_replay_runs_per_project: 15\n    max_replay_runs_per_dag: 5\n")),(0,a.kt)("p",null,"Lets say there is only 15 Replay slots at a time, and there are 40 runs requests comes. Replay will clear the first 15,\nput the rest 25 in queue, and Replay will keep checking on some interval, of whether the process has been completed and\ncan take another runs."),(0,a.kt)("p",null,"Picking which runs to be processed:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Based on Replay request time (FIFO)"),(0,a.kt)("li",{parentName:"ul"},"Based on the slot per dag. If the overall is set to 15 but the slot per dag is only 5, then a request to replay a job\nthat has 10 runs cannot be done in one go.")),(0,a.kt)("p",null,"There should be a default value for each of the configuration."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Max_replay_runs_per_project: default 10"),(0,a.kt)("li",{parentName:"ul"},"Max_replay_runs_per_dag: default 5")),(0,a.kt)("p",null,"Optimus will check on how many Replay job is currently running, and how many task run is running for each job. Additional\nchanges might be needed on Replay side for optimization, to avoid calling Airflow API in every interval (Replay syncer\nresponsibility) and fetch data from Replay table instead."))}d.isMDXComponent=!0}}]);