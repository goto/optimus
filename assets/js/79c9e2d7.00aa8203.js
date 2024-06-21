"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6449],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>m});var o=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function r(e,t){if(null==e)return{};var n,o,i=function(e,t){if(null==e)return{};var n,o,i={},l=Object.keys(e);for(o=0;o<l.length;o++)n=l[o],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(o=0;o<l.length;o++)n=l[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=o.createContext({}),p=function(e){var t=o.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},d=function(e){var t=p(e.components);return o.createElement(s.Provider,{value:t},e.children)},c="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},h=o.forwardRef((function(e,t){var n=e.components,i=e.mdxType,l=e.originalType,s=e.parentName,d=r(e,["components","mdxType","originalType","parentName"]),c=p(n),h=i,m=c["".concat(s,".").concat(h)]||c[h]||u[h]||l;return n?o.createElement(m,a(a({ref:t},d),{},{components:n})):o.createElement(m,a({ref:t},d))}));function m(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var l=n.length,a=new Array(l);a[0]=h;var r={};for(var s in t)hasOwnProperty.call(t,s)&&(r[s]=t[s]);r.originalType=e,r[c]="string"==typeof e?e:i,a[1]=r;for(var p=2;p<l;p++)a[p]=n[p];return o.createElement.apply(null,a)}return o.createElement.apply(null,n)}h.displayName="MDXCreateElement"},3283:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>a,default:()=>u,frontMatter:()=>l,metadata:()=>r,toc:()=>p});var o=n(7462),i=(n(7294),n(3905));const l={},a=void 0,r={unversionedId:"rfcs/improving_time_and_flow_of_deployment",id:"rfcs/improving_time_and_flow_of_deployment",title:"improving_time_and_flow_of_deployment",description:"- Feature Name: Improve Time & Flow of the core Optimus Job Deployment, Replay, and Backup",source:"@site/docs/rfcs/20220216_improving_time_and_flow_of_deployment.md",sourceDirName:"rfcs",slug:"/rfcs/improving_time_and_flow_of_deployment",permalink:"/optimus/docs/rfcs/improving_time_and_flow_of_deployment",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/rfcs/20220216_improving_time_and_flow_of_deployment.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1718957861,formattedLastUpdatedAt:"Jun 21, 2024",sidebarPosition:20220216,frontMatter:{}},s={},p=[{value:"Background :",id:"background-",level:2},{value:"Deploy",id:"deploy",level:3},{value:"Replay Request",id:"replay-request",level:3},{value:"Backup Request",id:"backup-request",level:3},{value:"Expected Behavior",id:"expected-behavior",level:2},{value:"Deploy Job",id:"deploy-job",level:3},{value:"Create Job",id:"create-job",level:3},{value:"Delete Job",id:"delete-job",level:3},{value:"Refresh Job",id:"refresh-job",level:3},{value:"Create Resource",id:"create-resource",level:3},{value:"Delete Resource",id:"delete-resource",level:3},{value:"Replay &amp; Backup",id:"replay--backup",level:3},{value:"Approach :",id:"approach-",level:2},{value:"Checking which jobs are modified?",id:"checking-which-jobs-are-modified",level:3},{value:"Persistence",id:"persistence",level:3},{value:"Event-Based Mechanism in Deployment",id:"event-based-mechanism-in-deployment",level:3},{value:"Handling Dependency Resolution Failure",id:"handling-dependency-resolution-failure",level:3},{value:"Handling Modified View",id:"handling-modified-view",level:3},{value:"CLI Perspective",id:"cli-perspective",level:3},{value:"Other Thoughts:",id:"other-thoughts",level:2}],d={toc:p},c="wrapper";function u(e){let{components:t,...n}=e;return(0,i.kt)(c,(0,o.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Feature Name: Improve Time & Flow of the core Optimus Job Deployment, Replay, and Backup"),(0,i.kt)("li",{parentName:"ul"},"Status: Draft"),(0,i.kt)("li",{parentName:"ul"},"Start Date: 2022-02-16"),(0,i.kt)("li",{parentName:"ul"},"Authors: Arinda & Sravan")),(0,i.kt)("h1",{id:"summary"},"Summary"),(0,i.kt)("p",null,"It is observed that the deployment of a project with more than 1000 jobs took around 6 minutes to complete, and the\nreplay request for the same project took around 4 minutes. An analysis to improve the time taken for this is needed,\nespecially if the project will be broke down to multiple namespaces. This will cause problem, as 10 namespaces might\ncan consume 6 minutes * 10 times."),(0,i.kt)("h1",{id:"technical-design"},"Technical Design"),(0,i.kt)("h2",{id:"background-"},"Background :"),(0,i.kt)("p",null,"Understanding the current process of the mentioned issues:"),(0,i.kt)("h3",{id:"deploy"},"Deploy"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Resolving dependencies for all the jobs in the requested project"),(0,i.kt)("li",{parentName:"ul"},"Resolving priorities for all the jobs"),(0,i.kt)("li",{parentName:"ul"},"Compiling the jobs within requested namespace"),(0,i.kt)("li",{parentName:"ul"},"Uploading compiled jobs to storage")),(0,i.kt)("h3",{id:"replay-request"},"Replay Request"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Resolving dependencies for all the jobs in the requested project"),(0,i.kt)("li",{parentName:"ul"},"Clearing scheduler dag run(s)")),(0,i.kt)("h3",{id:"backup-request"},"Backup Request"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Resolving dependencies for all the jobs in the requested project"),(0,i.kt)("li",{parentName:"ul"},"Duplicate table")),(0,i.kt)("p",null,"All the processes above need dependency resolution. When resolving dependency, it is being done for ALL the jobs in the\nproject, regardless of the namespace and regardless if it has changed or not. For every job (bq2bq for example), Optimus\nwill call each of the jobs the GenerateDependencies function in the plugin, and do a dry run hitting the Bigquery API.\nThis process has been done in parallel."),(0,i.kt)("p",null,"To simulate, let\u2019s say there are 1000 bq2bq jobs in a project."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"    Job     |   Upstream\n--------------------------\n    A       |   -\n    B       |   A\n    C       |   A, B\n    D       |   C       \n    ...     |   ...\n")),(0,i.kt)("p",null,"There is a change in job C, that it no longer has dependency to job A. When it happens, when deploying, currently\nOptimus will resolve dependencies for all 1000 jobs. While in fact, the changed dependencies will only happen for job C.\nThere is only a slight difference where upstream is no longer A and B but only B."),(0,i.kt)("p",null,"Currently, Optimus is resolving dependencies every time it is needed because it is not stored anywhere, due to keep\nchanging dependencies. However, now we are seeing a timing problem, and the fact that not all jobs need to be dependency\nresolved everytime there\u2019s a deployment, a modification can be considered."),(0,i.kt)("p",null,"As part of this issue, we are also revisiting the current flow of job deployment process."),(0,i.kt)("h2",{id:"expected-behavior"},"Expected Behavior"),(0,i.kt)("h3",{id:"deploy-job"},"Deploy Job"),(0,i.kt)("p",null,"Accepts the whole state of the namespace/project. What is not available will be deleted."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Identify the added / modified / deleted jobs"),(0,i.kt)("li",{parentName:"ul"},"Resolve dependency only for the added or modified jobs and persist the dependency to DB"),(0,i.kt)("li",{parentName:"ul"},"Do priority resolution for all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Compile all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Upload the compiled jobs")),(0,i.kt)("p",null,"The difference between the expected behavior and current implementation is that we will only resolve dependency for\nwhat are necessary, and we will compile all the jobs in the project regardless the namespace. Compile and deploy all\njobs in the project is necessary to avoid below use case:"),(0,i.kt)("p",null,"Let's say in a single project, lies these 4 jobs. Job C depend on Job B, job B depend on Job A, and Job A and Job D are\nindependent. Notice that Job C is in a different namespace."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"Job A (Namespace 1)             : weight 100\n|-- Job B (Namespace 1)         : weight 90\n|   |-- Job C (Namespace 2)     : weight 80\n|-- Job D (Namespace 1)         : weight 100\n")),(0,i.kt)("p",null,"Now let's say Job E (Namespace 1) is introduced and Job B is no longer depend directly on Job A, but instead to the new\nJob E."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"Job A (Namespace 1)             : weight 100\n|-- Job E (Namespace 1)         : weight 90\n|   |-- Job B (Namespace 1)     : weight 80\n|       |-- Job C (Namespace 2) : weight 70\n|-- Job D (Namespace 1)         : weight 100\n")),(0,i.kt)("p",null,"Notice that Job C priority weight has been changed. This example shows that even though the changes are in Namespace 1,\nthe other namespace is also affected and needs to be recompiled and deployed."),(0,i.kt)("h3",{id:"create-job"},"Create Job"),(0,i.kt)("p",null,"Accept a single/multiple jobs to be created and deployed."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Resolve dependency for the requested job and persist the dependency to DB"),(0,i.kt)("li",{parentName:"ul"},"Do priority resolution for all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Compile all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Upload the compiled jobs")),(0,i.kt)("h3",{id:"delete-job"},"Delete Job"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Identify any dependent jobs using dependencies table"),(0,i.kt)("li",{parentName:"ul"},"Delete only if there are no dependencies\nTBD: Doing soft delete or move the deleted jobs to a different table")),(0,i.kt)("h3",{id:"refresh-job"},"Refresh Job"),(0,i.kt)("p",null,"Using current state of job that has been stored, redo the dependency resolution, recompile, redeploy.\nCan be useful to do clean deploy or upgrading jobs."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Resolve dependency for all jobs in the namespace/project and persist to DB"),(0,i.kt)("li",{parentName:"ul"},"Do priority resolution for all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Compile all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Upload the compiled jobs")),(0,i.kt)("h3",{id:"create-resource"},"Create Resource"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Deploy the requested resource"),(0,i.kt)("li",{parentName:"ul"},"Identify jobs that are dependent to the resource"),(0,i.kt)("li",{parentName:"ul"},"Resolve dependency for the jobs found"),(0,i.kt)("li",{parentName:"ul"},"Compile all jobs in the project"),(0,i.kt)("li",{parentName:"ul"},"Upload the compiled jobs\nAn explanation of this behaviour can be found in ",(0,i.kt)("inlineCode",{parentName:"li"},"Handling Modified View")," section")),(0,i.kt)("h3",{id:"delete-resource"},"Delete Resource"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Identify jobs that are dependent to the requested resource"),(0,i.kt)("li",{parentName:"ul"},"Delete only if there are no dependencies")),(0,i.kt)("h3",{id:"replay--backup"},"Replay & Backup"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Get the dependencies from the dependencies table."),(0,i.kt)("li",{parentName:"ul"},"Continue to build the tree.")),(0,i.kt)("h2",{id:"approach-"},"Approach :"),(0,i.kt)("h3",{id:"checking-which-jobs-are-modified"},"Checking which jobs are modified?"),(0,i.kt)("p",null,"Currently, Optimus receives all the jobs to be deployed, compares which one to be deleted and which one to keep,\nresolves and compiles them all. Optimus does not know the state of which changed."),(0,i.kt)("p",null,"One of the possibilities is by using Job hash. Fetch the jobs from DB, hash and compare with the one requested."),(0,i.kt)("h3",{id:"persistence"},"Persistence"),(0,i.kt)("p",null,"The process can be optimized only if the dependencies are stored, so no need to resolve it all every time it is needed.\nCurrently, this is the struct of JobSpec in Optimus:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-go"},"type JobSpec struct {\n    ID             uuid.UUID\n    Name           string\n    Dependencies   map[string]JobSpecDependency\n    ....\n}\n\ntype JobSpecDependency struct {\n    Project *ProjectSpec\n    Job     *JobSpec\n    Type    JobSpecDependencyType\n}\n")),(0,i.kt)("p",null,"The Dependencies field will be filled with inferred dependency after dependency resolution is finished."),(0,i.kt)("p",null,"We can have a new table to persist the job ID dependency."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"    job_id              |   UUID\n    job_dependency_id   |   UUID\n")),(0,i.kt)("p",null,"Example"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"    Job     |   Upstream\n---------------------------\n    A       |   -\n    B       |   A\n    C       |   A\n    C       |   B\n    D       |   C\n    ...     |   ...\n")),(0,i.kt)("p",null,"If now C has been modified to have upstream of only B, means:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Record with jobID C will be deleted"),(0,i.kt)("li",{parentName:"ul"},"Insert 1 new record: C with dependency B")),(0,i.kt)("p",null,"Advantages:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Data is available even though there are pod restarts."),(0,i.kt)("li",{parentName:"ul"},"Better visibility of current dependencies.")),(0,i.kt)("p",null,"Disadvantages:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Additional time to write/read from DB")),(0,i.kt)("h3",{id:"event-based-mechanism-in-deployment"},"Event-Based Mechanism in Deployment"),(0,i.kt)("p",null,"Revisiting the process of deployment:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"Step                |   Deploy      | Create Job    | Refresh\n---------------------------------------------------------------\nResolve dependency  |   Diff        | Requested     | All\nResolve priority    |   All         | All           | All\nCompile             |   All         | All           | All\nUpload              |   All         | All           | All\n")),(0,i.kt)("p",null,"Notice that priority resolution, compilation, and upload compiled jobs needs to be done for all the jobs in the project\nfor all the namespaces. Each of the request can be done multiple times per minute and improvisation to speed up the\nprocess is needed."),(0,i.kt)("p",null,"Whenever there is a request to do deployment, job creation, and refresh, Optimus will do dependency resolution based\non each of the cases. After it finishes, it will push an event to be picked by a worker to do priority resolution,\ncompilation, and upload asynchronously. There will be deduplication in the event coming in, to avoid doing duplicated\nprocess."),(0,i.kt)("p",null,"There will be a get deployment status API introduced to poll whether these async processes has been finished or not."),(0,i.kt)("h3",{id:"handling-dependency-resolution-failure"},"Handling Dependency Resolution Failure"),(0,i.kt)("p",null,"Currently, whenever there is a single jobs that is failing in dependency resolution, and it is within the same\nnamespace as requested, it will fail the entire process. We are avoiding the entire deployment pipeline to be blocked\nby a single job failure, but instead sending it as part of the response and proceeding the deployment until finished.\nOnly the failed jobs will not be deployed. There will be metrics being added to add more visibility around this."),(0,i.kt)("h3",{id:"handling-modified-view"},"Handling Modified View"),(0,i.kt)("p",null,"A BQ2BQ job can have a source from views. For this job, the dependency will be the underlying tables of the view. Let's\nsimulate a case where there is a change in the view source."),(0,i.kt)("p",null,"In a project, there is view ",(0,i.kt)("inlineCode",{parentName:"p"},"X")," that querying from table ",(0,i.kt)("inlineCode",{parentName:"p"},"A")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"B"),". There is also table ",(0,i.kt)("inlineCode",{parentName:"p"},"C")," that querying from View\n",(0,i.kt)("inlineCode",{parentName:"p"},"X"),". The job dependencies for this case can be summarized as:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"    Job     |   Upstream\n---------------------------\n    A       |   -\n    B       |   -\n    C       |   A\n    C       |   B\n")),(0,i.kt)("p",null,"Job C has dependency to job ",(0,i.kt)("inlineCode",{parentName:"p"},"A")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"B"),", even though it is querying from view ",(0,i.kt)("inlineCode",{parentName:"p"},"X"),"."),(0,i.kt)("p",null,"Imagine a case where view ",(0,i.kt)("inlineCode",{parentName:"p"},"X")," is modified, for example no longer querying from ",(0,i.kt)("inlineCode",{parentName:"p"},"A")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"B"),", but instead only from ",(0,i.kt)("inlineCode",{parentName:"p"},"A"),".\nJob ",(0,i.kt)("inlineCode",{parentName:"p"},"C")," dependency will never be updated, since it is not considered as modified. There should be a mechanism where if\na view is updated, it will also resolve the dependency for the jobs that depend on the view."),(0,i.kt)("p",null,"To make this happen, there should be a visibility of which resources are the sources of a job, for example which job is\nusing this view as a destination and querying from this view. Optimus is a transformation tool, in the job spec we store\nwhat is the transformation destination of the job. However, we are not storing what are the sources of the transformation.\nThe only thing we have is job dependency, not resource."),(0,i.kt)("p",null,"We can add a Source URNs field to the jobs specs, or create a Job Source table. Whenever there is a change in a view\nthrough Optimus, datastore should be able to request the dependency resolution for the view's dependent and having the\ndependencies updated. We will also provide the mechanism to refresh jobs."),(0,i.kt)("h3",{id:"cli-perspective"},"CLI Perspective"),(0,i.kt)("p",null,"Deploy job per namespace (using DeployJobSpecification rpc)"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"optimus job deploy --namespace --project\n")),(0,i.kt)("p",null,"Deploy job for selected jobs (using CreateJobSpecification rpc)"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"optimus job deploy --namespace --project --jobs=(job1,job2)\n")),(0,i.kt)("p",null,"Refresh the entire namespace/project"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"optimus job refresh --namespace --project\n")),(0,i.kt)("p",null,"Refresh the selected jobs"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"optimus job refresh --namespace --project --jobs=(job1,job2)\n")),(0,i.kt)("h2",{id:"other-thoughts"},"Other Thoughts:"),(0,i.kt)("p",null,"Cache Considerations instead of persisting to PG"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Might be faster as there is no additional time for write/read from DB"),(0,i.kt)("li",{parentName:"ul"},"Data will be unavailable post pod restarts. Need to redo the dependency resolution overall"),(0,i.kt)("li",{parentName:"ul"},"Poor visibility")))}u.isMDXComponent=!0}}]);