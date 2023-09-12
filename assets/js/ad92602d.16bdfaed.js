"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6374],{3905:(e,n,t)=>{t.d(n,{Zo:()=>c,kt:()=>f});var a=t(7294);function r(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function i(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);n&&(a=a.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,a)}return t}function o(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?i(Object(t),!0).forEach((function(n){r(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):i(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function s(e,n){if(null==e)return{};var t,a,r=function(e,n){if(null==e)return{};var t,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)t=i[a],n.indexOf(t)>=0||(r[t]=e[t]);return r}(e,n);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)t=i[a],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var l=a.createContext({}),p=function(e){var n=a.useContext(l),t=n;return e&&(t="function"==typeof e?e(n):o(o({},n),e)),t},c=function(e){var n=p(e.components);return a.createElement(l.Provider,{value:n},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var n=e.children;return a.createElement(a.Fragment,{},n)}},m=a.forwardRef((function(e,n){var t=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=p(t),m=r,f=u["".concat(l,".").concat(m)]||u[m]||d[m]||i;return t?a.createElement(f,o(o({ref:n},c),{},{components:t})):a.createElement(f,o({ref:n},c))}));function f(e,n){var t=arguments,r=n&&n.mdxType;if("string"==typeof e||r){var i=t.length,o=new Array(i);o[0]=m;var s={};for(var l in n)hasOwnProperty.call(n,l)&&(s[l]=n[l]);s.originalType=e,s[u]="string"==typeof e?e:r,o[1]=s;for(var p=2;p<i;p++)o[p]=t[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,t)}m.displayName="MDXCreateElement"},385:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>s,toc:()=>p});var a=t(7462),r=(t(7294),t(3905));const i={},o="Organizing Specifications",s={unversionedId:"client-guide/organizing-specifications",id:"client-guide/organizing-specifications",title:"Organizing Specifications",description:"Optimus supports two ways to deploy specifications",source:"@site/docs/client-guide/organizing-specifications.md",sourceDirName:"client-guide",slug:"/client-guide/organizing-specifications",permalink:"/optimus/docs/client-guide/organizing-specifications",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/organizing-specifications.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1694508382,formattedLastUpdatedAt:"Sep 12, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Uploading Job to Scheduler",permalink:"/optimus/docs/client-guide/uploading-jobs-to-scheduler"},next:{title:"Backup BigQuery Resource",permalink:"/optimus/docs/client-guide/backup-bigquery-resource"}},l={},p=[],c={toc:p},u="wrapper";function d(e){let{components:n,...t}=e;return(0,r.kt)(u,(0,a.Z)({},c,t,{components:n,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"organizing-specifications"},"Organizing Specifications"),(0,r.kt)("p",null,"Optimus supports two ways to deploy specifications"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"REST/GRPC"),(0,r.kt)("li",{parentName:"ul"},"Optimus CLI deploy command")),(0,r.kt)("p",null,"When using Optimus CLI to deploy, either manually or from a CI pipeline, it is advised to use a version control system\nlike git. Here is a simple directory structure that can be used as a template for jobs and datastore resources,\nassuming there are 2 namespaces in a project."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},".\n\u251c\u2500\u2500 optimus.yaml\n\u251c\u2500\u2500 preset.yaml\n\u251c\u2500\u2500 README.md\n\u251c\u2500\u2500 namespace-1\n\u2502   \u251c\u2500\u2500 jobs\n|   \u2502   \u251c\u2500\u2500 job1\n|   \u2502   \u251c\u2500\u2500 job2\n|   \u2502   \u2514\u2500\u2500 this.yaml\n\u2502   \u2514\u2500\u2500 resources\n|       \u251c\u2500\u2500 bigquery\n\u2502       \u2502   \u251c\u2500\u2500 table1\n\u2502       \u2502   \u251c\u2500\u2500 table2\n|       |   \u2514\u2500\u2500 this.yaml\n\u2502       \u2514\u2500\u2500 postgres\n\u2502           \u2514\u2500\u2500 table1\n\u251c\u2500\u2500 namespace-2\n\u251c\u2500\u2500 jobs\n\u2514\u2500\u2500 resources\n")),(0,r.kt)("p",null,"You might have also noticed there are ",(0,r.kt)("inlineCode",{parentName:"p"},"this.yaml")," files being used in some directories. This file is used to share a\nsingle set of configurations across multiple sub-directories. For example, if you create a file at\n/namespace-1/jobs/this.yaml, then all subdirectories inside /namespaces-1/jobs will inherit this config as defaults.\nIf the same config is specified in subdirectory, then subdirectory will override the parent defaults."),(0,r.kt)("p",null,"For example a this.yaml in ",(0,r.kt)("inlineCode",{parentName:"p"},"/namespace-1/jobs")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nschedule:\n  interval: @daily\ntask:\n  name: bq2bq\n  config:\n    BQ_SERVICE_ACCOUNT: "{{.secret.BQ_SERVICE_ACCOUNT}}"\nbehavior:\n  depends_on_past: false\n  retry:\n    count: 1\n    delay: 5s\n')),(0,r.kt)("p",null,"and a job.yaml in ",(0,r.kt)("inlineCode",{parentName:"p"},"/namespace-1/jobs/job1")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'name: sample_replace\nowner: optimus@example.io\nschedule:\n  start_date: "2020-09-25"\n  interval: 0 10 * * *\nbehavior:\n  depends_on_past: true\ntask:\n  name: bq2bq\n  config:\n    project: project_name\n    dataset: project_dataset\n    table: sample_replace\n    load_method: REPLACE\n  window:\n    size: 48h\n    offset: 24h\n')),(0,r.kt)("p",null,"will result in final computed job.yaml during deployment as"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: sample_replace\nowner: optimus@example.io\nschedule:\n  start_date: "2020-10-06"\n  interval: 0 10 * * *\nbehavior:\n  depends_on_past: true\n  retry:\n    count: 1\n    delay: 5s\ntask:\n  name: bq2bq\n  config:\n    project: project_name\n    dataset: project_dataset\n    table: sample_replace\n    load_method: REPLACE\n    BQ_SERVICE_ACCOUNT: "{{.secret.BQ_SERVICE_ACCOUNT}}"\n  window:\n    size: 48h\n    offset: 24h\n')),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Preset (since v0.10.0)")),(0,r.kt)("p",null,"Window preset allows for easier usage of window configuration and can be specified through a YAML file. It is optional in nature and is recommended to be put in the same directory as ",(0,r.kt)("inlineCode",{parentName:"p"},"optimus.yaml")," if being set. For more information on how to utilize window preset, please check ",(0,r.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/intervals-and-windows"},"this page"),"."))}d.isMDXComponent=!0}}]);