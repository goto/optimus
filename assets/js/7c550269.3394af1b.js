"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[253],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>g});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),c=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(s.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=c(n),d=r,g=u["".concat(s,".").concat(d)]||u[d]||m[d]||o;return n?a.createElement(g,i(i({ref:t},p),{},{components:n})):a.createElement(g,i({ref:t},p))}));function g(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:r,i[1]=l;for(var c=2;c<o;c++)i[c]=n[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},9537:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>m,frontMatter:()=>o,metadata:()=>l,toc:()=>c});var a=n(7462),r=(n(7294),n(3905));const o={},i="Configuration",l={unversionedId:"client-guide/configuration",id:"client-guide/configuration",title:"Configuration",description:"Client configuration holds the necessary information for connecting to the Optimus server as well as for specification",source:"@site/docs/client-guide/configuration.md",sourceDirName:"client-guide",slug:"/client-guide/configuration",permalink:"/optimus/docs/client-guide/configuration",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/configuration.md",tags:[],version:"current",lastUpdatedBy:"Anwar Hidayat",lastUpdatedAt:1688612861,formattedLastUpdatedAt:"Jul 6, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"DB Migrations",permalink:"/optimus/docs/server-guide/db-migrations"},next:{title:"Managing Project & Namespace",permalink:"/optimus/docs/client-guide/managing-project-namespace"}},s={},c=[{value:"Project",id:"project",level:2},{value:"Namespaces",id:"namespaces",level:2}],p={toc:c},u="wrapper";function m(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"configuration"},"Configuration"),(0,r.kt)("p",null,"Client configuration holds the necessary information for connecting to the Optimus server as well as for specification\ncreation. Optimus provides a way for you to initialize the client configuration by using the ",(0,r.kt)("inlineCode",{parentName:"p"},"init")," command. Go to the\ndirectory where you want to have your Optimus specifications. Run the below command and answer the prompt questions:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus init\n\n? What is the Optimus service host? localhost:9100\n? What is the Optimus project name? sample_project\n? What is the namespace name? sample_namespace\n? What is the type of data store for this namespace? bigquery\n? Do you want to add another namespace? No\nClient config is initialized successfully\n")),(0,r.kt)("p",null,"After running the init command, the Optimus client config will be configured. Along with it, the directories for the\nchosen namespaces, including the sub-directories for jobs and resources will be created with the following structure:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"sample_project\n\u251c\u2500\u2500 sample_namespace\n\u2502   \u2514\u2500\u2500 jobs\n\u2502   \u2514\u2500\u2500 resources\n\u2514\u2500\u2500 optimus.yaml\n")),(0,r.kt)("p",null,"Below is the client configuration that has been generated:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nlog:\n  level: INFO\n  format: ""\nhost: localhost:9100\nproject:\n  name: sample_project\n  config: {}\nnamespaces:\n- name: sample_namespace\n  config: {}\n  job:\n    path: sample_namespace/jobs\n  datastore:\n    - type: bigquery\n      path: sample_namespace/resources\n      backup: {}\n')),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Configuration"),(0,r.kt)("th",{parentName:"tr",align:null},"Description"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Version"),(0,r.kt)("td",{parentName:"tr",align:null},"Supports only version 1 at the moment.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Log"),(0,r.kt)("td",{parentName:"tr",align:null},"Logging level & format configuration")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Host"),(0,r.kt)("td",{parentName:"tr",align:null},"Optimus server host")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Project"),(0,r.kt)("td",{parentName:"tr",align:null},"Chosen Optimus project name and configurations.")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Namespaces"),(0,r.kt)("td",{parentName:"tr",align:null},"Namespaces that are owned by the project.")))),(0,r.kt)("h2",{id:"project"},"Project"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Project name should be unique."),(0,r.kt)("li",{parentName:"ul"},"Several configs are mandatory for job compilation and deployment use case:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"storage_path")," config to store the job compilation result. A path can be anything, for example, a local directory\npath or a Google Cloud Storage path."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"scheduler_host")," being used for job execution and sensors."),(0,r.kt)("li",{parentName:"ul"},"Specific secrets might be needed for the above configs. Take a look at the detail ",(0,r.kt)("a",{parentName:"li",href:"/optimus/docs/client-guide/managing-secrets"},"here"),"."))),(0,r.kt)("li",{parentName:"ul"},"You can put any other project configurations which can be used in job specifications.")),(0,r.kt)("h2",{id:"namespaces"},"Namespaces"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Name should be unique in the project."),(0,r.kt)("li",{parentName:"ul"},"You can put any namespace configurations which can be used in specifications."),(0,r.kt)("li",{parentName:"ul"},"Job path needs to be properly set so Optimus CLI will able to find all of your job specifications to be processed."),(0,r.kt)("li",{parentName:"ul"},"For datastore, currently Optimus only accepts ",(0,r.kt)("inlineCode",{parentName:"li"},"bigquery")," datastore type and you need to set the specification path\nfor this. Also, there is an optional ",(0,r.kt)("inlineCode",{parentName:"li"},"backup")," config map. Take a look at the backup guide section ",(0,r.kt)("a",{parentName:"li",href:"/optimus/docs/client-guide/backup-bigquery-resource"},"here"),"\nto understand more about this.")))}m.isMDXComponent=!0}}]);