"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[275],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>h});var i=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function n(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,i)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?n(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):n(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,i,r=function(e,t){if(null==e)return{};var a,i,r={},n=Object.keys(e);for(i=0;i<n.length;i++)a=n[i],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);for(i=0;i<n.length;i++)a=n[i],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=i.createContext({}),u=function(e){var t=i.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=u(e.components);return i.createElement(s.Provider,{value:t},e.children)},m="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},c=i.forwardRef((function(e,t){var a=e.components,r=e.mdxType,n=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),m=u(a),c=r,h=m["".concat(s,".").concat(c)]||m[c]||d[c]||n;return a?i.createElement(h,l(l({ref:t},p),{},{components:a})):i.createElement(h,l({ref:t},p))}));function h(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var n=a.length,l=new Array(n);l[0]=c;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o[m]="string"==typeof e?e:r,l[1]=o;for(var u=2;u<n;u++)l[u]=a[u];return i.createElement.apply(null,l)}return i.createElement.apply(null,a)}c.displayName="MDXCreateElement"},8865:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>d,frontMatter:()=>n,metadata:()=>o,toc:()=>u});var i=a(7462),r=(a(7294),a(3905));const n={},l=void 0,o={unversionedId:"rfcs/optimus_dashboarding",id:"rfcs/optimus_dashboarding",title:"optimus_dashboarding",description:"- Feature Name: Optimus Dashboarding",source:"@site/docs/rfcs/20220517_optimus_dashboarding.md",sourceDirName:"rfcs",slug:"/rfcs/optimus_dashboarding",permalink:"/optimus/docs/rfcs/optimus_dashboarding",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/rfcs/20220517_optimus_dashboarding.md",tags:[],version:"current",lastUpdatedBy:"Oky Setiawan",lastUpdatedAt:1715692439,formattedLastUpdatedAt:"May 14, 2024",sidebarPosition:20220517,frontMatter:{}},s={},u=[{value:"Job Metrics dashboard (aka. Job sumaries page):",id:"job-metrics-dashboard-aka-job-sumaries-page",level:2},{value:"Job lineage view :",id:"job-lineage-view-",level:2},{value:"Approach : Collect info from Airflow events / callbacks :",id:"approach--collect-info-from-airflow-events--callbacks-",level:2},{value:"Optimus Perspective DB model:",id:"optimus-perspective-db-model",level:2},{value:"Compute SLA miss:",id:"compute-sla-miss",level:2},{value:"SLA Definition :",id:"sla-definition-",level:3},{value:"Approach For SLA dashboarding:",id:"approach-for-sla-dashboarding",level:3},{value:"Approach For SLA alerting:",id:"approach-for-sla-alerting",level:3},{value:"Other Considerations:",id:"other-considerations",level:2},{value:"Terminology:",id:"terminology",level:2}],p={toc:u},m="wrapper";function d(e){let{components:t,...a}=e;return(0,r.kt)(m,(0,i.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Feature Name: Optimus Dashboarding"),(0,r.kt)("li",{parentName:"ul"},"Status: Draft"),(0,r.kt)("li",{parentName:"ul"},"Start Date: 2022-05-17"),(0,r.kt)("li",{parentName:"ul"},"Authors: Yash Bhardwaj")),(0,r.kt)("h1",{id:"summary"},"Summary"),(0,r.kt)("p",null,"Optimus users need to frequestly visit airflow webserver which does not provide a optimus perspective of the jobs i.e. is lacks the leniage view and project / namespace level clasification."),(0,r.kt)("p",null,"The proposal here is to capture job related events and state information via airflow event callbacks and job lifecycle events ."),(0,r.kt)("h1",{id:"design"},"Design"),(0,r.kt)("h2",{id:"job-metrics-dashboard-aka-job-sumaries-page"},"Job Metrics dashboard (aka. Job sumaries page):"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Monitor jobs performance stats :",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Duaration per optimus job task/sensor/hook."),(0,r.kt)("li",{parentName:"ul"},"Job completion Sla misses trend."),(0,r.kt)("li",{parentName:"ul"},"Job summaries."))),(0,r.kt)("li",{parentName:"ul"},"Jobs groupings into namespaces and projects.")),(0,r.kt)("hr",null),(0,r.kt)("h2",{id:"job-lineage-view-"},"Job lineage view :"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Currently only columbus provides lineage view."),(0,r.kt)("li",{parentName:"ul"},"That does not reflect the job status and run level dependencies/relationships.")),(0,r.kt)("h2",{id:"approach--collect-info-from-airflow-events--callbacks-"},"Approach : Collect info from Airflow events / callbacks :"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Get ",(0,r.kt)("inlineCode",{parentName:"p"},"DAG")," lifecycle events from scheduler through:"),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Airflow triggered Callbacks",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"on_success_callback")," Invoked when the task succeeds"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"on_failure_callback")," Invoked when the task fails"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"sla_miss_callback")," Invoked when a task misses its defined SLA ( SLA here is scheduling delay not to be concused with job completion delay )"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"on_retry_callback")," Invoked when the task is up for retry"))),(0,r.kt)("li",{parentName:"ul"},"Events fired by our Custom logic added into ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Custom operator implimentation "),(0,r.kt)("li",{parentName:"ul"},"airflow job_start_event and job_end_event task"))))),(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("p",{parentName:"li"},"Information from these events is then relayed to the optimus server. Optimus then writes this into"))),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"OptimusDB"),"  : for summaries dashboarding and for powering lineage views (in future)")),(0,r.kt)("ol",{start:3},(0,r.kt)("li",{parentName:"ol"},"Reasons for choosing this approach")),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"This is less tightly coupled with the Current chosen Scheduler.",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"If later support is provided for more schedulers, exixting optimus data collection APIs and Optimus Data model can be reused to power our Frontend system.")))),(0,r.kt)("ol",{start:4},(0,r.kt)("li",{parentName:"ol"},"Known limitations:")),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Since this is an event based architecture, collection of Job/DAG state info will be hampered  in cases of panic failures for instance DAG python code throwing uncaught exception.",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"even in such cases we will be able to determine the SLA missing jobs ")))),(0,r.kt)("hr",null),(0,r.kt)("h2",{id:"optimus-perspective-db-model"},"Optimus Perspective DB model:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Optimus shall consider each scheduled run and its subsiquent retry as a new job_run "),(0,r.kt)("li",{parentName:"ul"},"sensor/task/hooks run information shall be grouped per job_run ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"To achive this, each sensor/task/hook Run is linked with the job_run.id and job_run.attempt_number"),(0,r.kt)("li",{parentName:"ul"},"while registering a sensor run the latest job_run.attempt for that given schedule time is used to link it."))),(0,r.kt)("li",{parentName:"ul"},"For each job_run(scheduled/re-run) there will only be one row per each sensor/task/hook registered in the Optimus DB.")),(0,r.kt)("hr",null),(0,r.kt)("h2",{id:"compute-sla-miss"},"Compute SLA miss:"),(0,r.kt)("h3",{id:"sla-definition-"},"SLA Definition :"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"if Time duration between the job ",(0,r.kt)("inlineCode",{parentName:"li"},"execution start time")," and ",(0,r.kt)("inlineCode",{parentName:"li"},"job completion time")," excedes the Defined SLA Limit, then that is termed as an SLA breach.")),(0,r.kt)("h3",{id:"approach-for-sla-dashboarding"},"Approach For SLA dashboarding:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Note Job Start Time"),(0,r.kt)("li",{parentName:"ul"},"With Each Job Run, associate the then SLA definition(user defined number in the job.yaml) of the job."),(0,r.kt)("li",{parentName:"ul"},"SLA breach are determined with the read QUERY of Grafana, which works as following ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SLA breach duration = ",(0,r.kt)("inlineCode",{parentName:"li"},"(min(job_end_time , time.now()) - job_start_time) - SLA_definition")))),(0,r.kt)("li",{parentName:"ul"},"Limitations",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Since this is an event based data collection setup is it possible that a job may have failed/crashed/hangged in such a situation optimus wont get the job finish callback, and hence cant determine the SLA breach ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"To work arround with that, at the time of job run registeration the end time is assumed to be a far future date. In case the job terminates properly we shall be able to determine the correct end_time , otherwise optimus is safe to assume that the job has not finised yet. The job Duration in such case will be the the time since the job has started running.")))))),(0,r.kt)("h3",{id:"approach-for-sla-alerting"},"Approach For SLA alerting:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Ariflow DAG level SLA definiation can be used to get DAG SLA breach event.",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"At the end of each task, a check is conducted to test whether the completed task\u2019s end-time exceeded the SLA OR the start time of the next task exceeded the SLA.")))),(0,r.kt)("hr",null),(0,r.kt)("h2",{id:"other-considerations"},"Other Considerations:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"contrary approached discussed ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Kubernetes Sidecar",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SideCar lifecycle hooks start/end "),(0,r.kt)("li",{parentName:"ul"},"sideCar to pull details from scheduler/plugin containers and push same to optimus server"))),(0,r.kt)("li",{parentName:"ul"},"Pull Approach",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Callbacks -> statsD to power job sumaries page"),(0,r.kt)("li",{parentName:"ul"},"access airflow API directly from Optimus to power job details view"))))),(0,r.kt)("li",{parentName:"ul"},"Future considerations ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Support for Events fired by Executor :",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"it is expected that even optimus-task and hooks shall independently be able to emit events to optimus notify api. this should help with improved executor observability.")))))),(0,r.kt)("h2",{id:"terminology"},"Terminology:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"Task")," Airflow task operator"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"Job")," Optimus job"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"DAG")," Airflow DAG")))}d.isMDXComponent=!0}}]);