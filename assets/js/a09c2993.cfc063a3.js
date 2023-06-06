"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[4128],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>h});var i=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,i,o=function(e,t){if(null==e)return{};var n,i,o={},r=Object.keys(e);for(i=0;i<r.length;i++)n=r[i],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(i=0;i<r.length;i++)n=r[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var u=i.createContext({}),l=function(e){var t=i.useContext(u),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},c=function(e){var t=l(e.components);return i.createElement(u.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},m=i.forwardRef((function(e,t){var n=e.components,o=e.mdxType,r=e.originalType,u=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),p=l(n),m=o,h=p["".concat(u,".").concat(m)]||p[m]||d[m]||r;return n?i.createElement(h,a(a({ref:t},c),{},{components:n})):i.createElement(h,a({ref:t},c))}));function h(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=n.length,a=new Array(r);a[0]=m;var s={};for(var u in t)hasOwnProperty.call(t,u)&&(s[u]=t[u]);s.originalType=e,s[p]="string"==typeof e?e:o,a[1]=s;for(var l=2;l<r;l++)a[l]=n[l];return i.createElement.apply(null,a)}return i.createElement.apply(null,n)}m.displayName="MDXCreateElement"},8495:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>u,contentTitle:()=>a,default:()=>d,frontMatter:()=>r,metadata:()=>s,toc:()=>l});var i=n(7462),o=(n(7294),n(3905));const r={id:"introduction",title:"Introduction"},a="Optimus",s={unversionedId:"introduction",id:"introduction",title:"Introduction",description:"Optimus is an ETL orchestration tool that helps manage data transformation jobs and manage warehouse resources.",source:"@site/docs/introduction.md",sourceDirName:".",slug:"/introduction",permalink:"/optimus/docs/introduction",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/introduction.md",tags:[],version:"current",lastUpdatedBy:"Anwar Hidayat",lastUpdatedAt:1686022872,formattedLastUpdatedAt:"Jun 6, 2023",frontMatter:{id:"introduction",title:"Introduction"},sidebar:"docsSidebar",next:{title:"Installation",permalink:"/optimus/docs/getting-started/installation"}},u={},l=[{value:"Multi-Tenancy Support",id:"multi-tenancy-support",level:2},{value:"Extensible",id:"extensible",level:2},{value:"Automated Dependency Resolution",id:"automated-dependency-resolution",level:2},{value:"In-Built Alerting",id:"in-built-alerting",level:2},{value:"Verification in Advance",id:"verification-in-advance",level:2}],c={toc:l},p="wrapper";function d(e){let{components:t,...r}=e;return(0,o.kt)(p,(0,i.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"optimus"},"Optimus"),(0,o.kt)("p",null,"Optimus is an ETL orchestration tool that helps manage data transformation jobs and manage warehouse resources.\nIt enables you to transform your data by writing the transformation script and YAML configuration while Optimus handles\nthe dependency, schedules it, and handles all other aspects of running transformation jobs at scale. Optimus also supports\nwarehouse resource management (currently BigQuery), which enables you to create, update, and read BigQuery tables, views, and datasets."),(0,o.kt)("p",null,(0,o.kt)("img",{alt:"High Level Optimus Diagram",src:n(2273).Z,title:"OptimusIntro",width:"1600",height:"745"})),(0,o.kt)("p",null,"Optimus was made to be extensible. Adding support for different kinds of sources/sinks and transformation executors\ncan be done easily. If your organization has to setup & manage data pipelines that are complex with multiple sources,\nsinks & there are many team members managing them, then Optimus is the perfect tool for you."),(0,o.kt)("h2",{id:"multi-tenancy-support"},"Multi-Tenancy Support"),(0,o.kt)("p",null,"Optimus supports multi-tenancy. Each tenant manages their own jobs, resources, secrets, and configuration while optimus\nmanaging dependencies across tenants."),(0,o.kt)("h2",{id:"extensible"},"Extensible"),(0,o.kt)("p",null,"Optimus provides the flexibility to you to define how your transformation jobs should behave, which data source or\nwarehouse sink you want to support, and what configurations you need from the users. This flexibility is addressed\nthrough ",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/plugin"},"plugin"),". At the moment, we provide a ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/goto/transformers/tree/main/task/bq2bq"},"BigQuery to BigQuery task plugin"),",\nbut you can write custom plugins such as Python transformations."),(0,o.kt)("p",null,"Also, in order to provide a unified command line experience of various tools, Optimus provides ",(0,o.kt)("a",{parentName:"p",href:"/optimus/docs/client-guide/work-with-extension"},"extensions"),"\nsupport on client side through which you can extend the capabilities for example providing governance. "),(0,o.kt)("h2",{id:"automated-dependency-resolution"},"Automated Dependency Resolution"),(0,o.kt)("p",null,"Optimus parses your data transformation queries and builds a dependency graph automatically without the user explicitly\ndefining the same. The dependencies are managed across tenants, so teams doesn\u2019t need to coordinate among themselves."),(0,o.kt)("h2",{id:"in-built-alerting"},"In-Built Alerting"),(0,o.kt)("p",null,"Always get notified when your job is not behaving as expected, on failures or on SLA misses. Optimus supports\nintegrations with slack & pagerduty."),(0,o.kt)("h2",{id:"verification-in-advance"},"Verification in Advance"),(0,o.kt)("p",null,"Minimize job runtime issues by validating and inspecting jobs before submitting them which enables them for faster\nturnaround time when submitting their jobs. Users can get to know about job dependencies, validation failures & some\nwarnings before submitting the jobs."))}d.isMDXComponent=!0},2273:(e,t,n)=>{n.d(t,{Z:()=>i});const i=n.p+"assets/images/OptimusIntro-0990771857ec459389c809e9c874e71a.png"}}]);