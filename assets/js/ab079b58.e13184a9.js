"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2994],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>f});var n=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var i=n.createContext({}),p=function(e){var t=n.useContext(i),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},c=function(e){var t=p(e.components);return n.createElement(i.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,i=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=p(a),d=o,f=u["".concat(i,".").concat(d)]||u[d]||m[d]||r;return a?n.createElement(f,l(l({ref:t},c),{},{components:a})):n.createElement(f,l({ref:t},c))}));function f(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,l=new Array(r);l[0]=d;var s={};for(var i in t)hasOwnProperty.call(t,i)&&(s[i]=t[i]);s.originalType=e,s[u]="string"==typeof e?e:o,l[1]=s;for(var p=2;p<r;p++)l[p]=a[p];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}d.displayName="MDXCreateElement"},7934:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>i,contentTitle:()=>l,default:()=>m,frontMatter:()=>r,metadata:()=>s,toc:()=>p});var n=a(7462),o=(a(7294),a(3905));const r={},l="Job",s={unversionedId:"concepts/job",id:"concepts/job",title:"Job",description:"A Job is the fundamental unit of the data pipeline which enables a data transformation in the warehouse of choice.",source:"@site/docs/concepts/job.md",sourceDirName:"concepts",slug:"/concepts/job",permalink:"/optimus/docs/concepts/job",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/job.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1720420004,formattedLastUpdatedAt:"Jul 8, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Resource",permalink:"/optimus/docs/concepts/resource"},next:{title:"Job Run",permalink:"/optimus/docs/concepts/job-run"}},i={},p=[{value:"Task",id:"task",level:2},{value:"Hook",id:"hook",level:2},{value:"Asset",id:"asset",level:2}],c={toc:p},u="wrapper";function m(e){let{components:t,...a}=e;return(0,o.kt)(u,(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"job"},"Job"),(0,o.kt)("p",null,"A Job is the fundamental unit of the data pipeline which enables a data transformation in the warehouse of choice.\nA user can configure various details mentioned below for the job:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Schedule interval"),(0,o.kt)("li",{parentName:"ul"},"Date from when a transformation should start executing"),(0,o.kt)("li",{parentName:"ul"},"Task & Hooks"),(0,o.kt)("li",{parentName:"ul"},"Assets needed for transformation"),(0,o.kt)("li",{parentName:"ul"},"Alerts")),(0,o.kt)("p",null,"Job specifications are being compiled to later be processed by the scheduler. Optimus is using Airflow as the scheduler,\nthus it is compiling the job specification to DAG (",(0,o.kt)("em",{parentName:"p"},"Directed Acryclic Graph"),") file."),(0,o.kt)("p",null,"Each of the DAG represents a single job, which consists of:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Airflow task(s). Transformation tasks and hooks will be compiled to Airflow tasks."),(0,o.kt)("li",{parentName:"ul"},"Sensors, only if the job has dependency.")),(0,o.kt)("p",null,"Each job has a single base transformation, we call them ",(0,o.kt)("strong",{parentName:"p"},"Task")," and might have the task pre or/and post operations,\nwhich are called ",(0,o.kt)("strong",{parentName:"p"},"Hooks"),"."),(0,o.kt)("h2",{id:"task"},"Task"),(0,o.kt)("p",null,"A task is a main transformation process that will fetch data, transform as configured, and sink to the destination.\nEach task has its own set of configs and can inherit configurations from a global configuration store."),(0,o.kt)("p",null,"Some examples of task are:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"BQ to BQ task"),(0,o.kt)("li",{parentName:"ul"},"BQ to Email task"),(0,o.kt)("li",{parentName:"ul"},"Python task"),(0,o.kt)("li",{parentName:"ul"},"Tableau task"),(0,o.kt)("li",{parentName:"ul"},"Etc.")),(0,o.kt)("h2",{id:"hook"},"Hook"),(0,o.kt)("p",null,"Hooks are the operations that you might want to run before or after a task. A hook is only associated with a single\nparent although they can depend on other hooks within the same job. There can be one or many or zero hooks for a Job as\nconfigured by the user. Some examples of hooks are:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://github.com/goto/predator"},"Predator")," (Profiling & Auditing for BQ)"),(0,o.kt)("li",{parentName:"ul"},"Publishing transformed data to Kafka"),(0,o.kt)("li",{parentName:"ul"},"Http Hooks")),(0,o.kt)("p",null,"Each hook has its own set of configs and shares the same asset folder as the base job. Hook can inherit configurations\nfrom the base transformation or from a global configuration store."),(0,o.kt)("p",null,"The fundamental difference between a hook and a task is, a task can have dependencies over other jobs inside the\nrepository whereas a hook can only depend on other hooks within the job."),(0,o.kt)("h2",{id:"asset"},"Asset"),(0,o.kt)("p",null,"There could be an asset folder along with the job.yaml file generated via optimus when a new job is created. This is a\nshared folder across base transformation task and all associated hooks. Assets can use macros and functions powered by\n",(0,o.kt)("a",{parentName:"p",href:"https://golang.org/pkg/text/template/"},"Go templating engine"),"."),(0,o.kt)("p",null,"Section of code can be imported from different asset files using template. For example:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"File partials.gtpl")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-gotemplate"},"DECLARE t1 TIMESTAMP;\n")),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Another file query.sql")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-gotemplate"},"{{template \"partials.gtpl\"}}\nSET t1 = '2021-02-10T10:00:00+00:00';\n")),(0,o.kt)("p",null,"During execution query.sql will be rendered as:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-gotemplate"},"DECLARE t1 TIMESTAMP;\nSET t1 = '2021-02-10T10:00:00+00:00';\n")),(0,o.kt)("p",null,"whereas ",(0,o.kt)("strong",{parentName:"p"},"partials.gtpl")," will be left as it is because file was saved with .gtpl extension."),(0,o.kt)("p",null,"Similarly, a single file can contain multiple blocks of code that can function as macro of code replacement. For example:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"file.data")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-gotemplate"},'Name: {{ template "name"}}, Gender: {{ template "gender" }}\n')),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"partials.gtpl")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-gotemplate"},'{{- define "name" -}} Adam {{- end}}\n{{- define "gender" -}} Male {{- end}}\n')),(0,o.kt)("p",null,"This will render file.data as"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"Name: Adam, Gender: Male\n")))}m.isMDXComponent=!0}}]);