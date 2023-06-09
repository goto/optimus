"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6886],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>h});var n=r(7294);function i(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){i(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,i=function(e,t){if(null==e)return{};var r,n,i={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(i[r]=e[r]);return i}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(i[r]=e[r])}return i}var c=n.createContext({}),l=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},u=function(e){var t=l(e.components);return n.createElement(c.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,i=e.mdxType,a=e.originalType,c=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),p=l(r),f=i,h=p["".concat(c,".").concat(f)]||p[f]||d[f]||a;return r?n.createElement(h,o(o({ref:t},u),{},{components:r})):n.createElement(h,o({ref:t},u))}));function h(e,t){var r=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var a=r.length,o=new Array(a);o[0]=f;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s[p]="string"==typeof e?e:i,o[1]=s;for(var l=2;l<a;l++)o[l]=r[l];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},4730:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>o,default:()=>d,frontMatter:()=>a,metadata:()=>s,toc:()=>l});var n=r(7462),i=(r(7294),r(3905));const a={},o="Architecture",s={unversionedId:"concepts/architecture",id:"concepts/architecture",title:"Architecture",description:"Architecture Diagram",source:"@site/docs/concepts/architecture.md",sourceDirName:"concepts",slug:"/concepts/architecture",permalink:"/optimus/docs/concepts/architecture",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/architecture.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1686289855,formattedLastUpdatedAt:"Jun 9, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Quickstart",permalink:"/optimus/docs/getting-started/quick-start"},next:{title:"Project",permalink:"/optimus/docs/concepts/project"}},c={},l=[{value:"CLI",id:"cli",level:2},{value:"Server",id:"server",level:2},{value:"Database",id:"database",level:2},{value:"Plugins",id:"plugins",level:2},{value:"Scheduler (Airflow)",id:"scheduler-airflow",level:2}],u={toc:l},p="wrapper";function d(e){let{components:t,...a}=e;return(0,i.kt)(p,(0,n.Z)({},u,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"architecture"},"Architecture"),(0,i.kt)("p",null,(0,i.kt)("img",{alt:"Architecture Diagram",src:r(3998).Z,title:"OptimusArchitecture",width:"1600",height:"1202"})),(0,i.kt)("h2",{id:"cli"},"CLI"),(0,i.kt)("p",null,"Optimus provides a command line interface to interact with the main optimus service and basic scaffolding job\nspecifications. It can be used to:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Start optimus server"),(0,i.kt)("li",{parentName:"ul"},"Create resource specifications for datastores"),(0,i.kt)("li",{parentName:"ul"},"Generate jobs & hooks based on user inputs"),(0,i.kt)("li",{parentName:"ul"},"Dump a compiled specification for the consumption of a scheduler"),(0,i.kt)("li",{parentName:"ul"},"Validate and inspect job specifications"),(0,i.kt)("li",{parentName:"ul"},"Deployment of specifications to Optimus Service")),(0,i.kt)("h2",{id:"server"},"Server"),(0,i.kt)("p",null,"Optimus Server handles all the client requests from direct end users or from airflow over http & grpc. The functionality\nof the server can be extended with the support of various plugins to various data sources & sinks. Everything around\njob/resource management is handled by the server except scheduling of jobs."),(0,i.kt)("h2",{id:"database"},"Database"),(0,i.kt)("p",null,"Optimus supports postgres as  the main storage backend. It is the source of truth for all user specifications,\nconfigurations, secrets, assets. It is the place where all the precomputed relations between jobs are stored."),(0,i.kt)("h2",{id:"plugins"},"Plugins"),(0,i.kt)("p",null,"Currently, Optimus doesn\u2019t hold any logic and is not responsible for handling any specific transformations. This\ncapability is extended through plugins and users can customize based on their needs on what plugins to use. Plugins can\nbe defined through a yaml specification. At the time of execution whatever the image that is configured in the plugin\nimage will be executed."),(0,i.kt)("h2",{id:"scheduler-airflow"},"Scheduler (Airflow)"),(0,i.kt)("p",null,"Scheduler is responsible for scheduling all the user defined jobs. Currently, optimus supports only Airflow as\nthe scheduler, support for more schedulers can be added."))}d.isMDXComponent=!0},3998:(e,t,r)=>{r.d(t,{Z:()=>n});const n=r.p+"assets/images/OptimusArchitecture-8c62094985b762ca9f1848f4976fdfee.png"}}]);