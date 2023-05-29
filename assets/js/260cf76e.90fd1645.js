"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[3541],{3905:(e,t,r)=>{r.d(t,{Zo:()=>s,kt:()=>m});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var u=n.createContext({}),p=function(e){var t=n.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},s=function(e){var t=p(e.components);return n.createElement(u.Provider,{value:t},e.children)},c="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},g=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,u=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),c=p(r),g=a,m=c["".concat(u,".").concat(g)]||c[g]||d[g]||o;return r?n.createElement(m,i(i({ref:t},s),{},{components:r})):n.createElement(m,i({ref:t},s))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=g;var l={};for(var u in t)hasOwnProperty.call(t,u)&&(l[u]=t[u]);l.originalType=e,l[c]="string"==typeof e?e:a,i[1]=l;for(var p=2;p<o;p++)i[p]=r[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}g.displayName="MDXCreateElement"},5886:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>u,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>l,toc:()=>p});var n=r(7462),a=(r(7294),r(3905));const o={},i="Server Configuration",l={unversionedId:"server-guide/configuration",id:"server-guide/configuration",title:"Server Configuration",description:"See the server configuration example on config.sample.yaml.",source:"@site/docs/server-guide/configuration.md",sourceDirName:"server-guide",slug:"/server-guide/configuration",permalink:"/optimus/docs/server-guide/configuration",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/server-guide/configuration.md",tags:[],version:"current",lastUpdatedBy:"Anwar Hidayat",lastUpdatedAt:1685365588,formattedLastUpdatedAt:"May 29, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Replay & Backup",permalink:"/optimus/docs/concepts/replay-and-backup"},next:{title:"Installing Plugins",permalink:"/optimus/docs/server-guide/installing-plugins"}},u={},p=[],s={toc:p},c="wrapper";function d(e){let{components:t,...r}=e;return(0,a.kt)(c,(0,n.Z)({},s,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"server-configuration"},"Server Configuration"),(0,a.kt)("p",null,"See the server configuration example on config.sample.yaml."),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Configuration"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Log"),(0,a.kt)("td",{parentName:"tr",align:null},"Logging level & format configuration.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Serve"),(0,a.kt)("td",{parentName:"tr",align:null},"Represents any configuration needed to start Optimus, such as port, host, DB details, and application key (for secrets encryption).")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Scheduler"),(0,a.kt)("td",{parentName:"tr",align:null},"Any scheduler-related configuration. Currently, Optimus only supports Airflow and has been set to default.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Telemetry"),(0,a.kt)("td",{parentName:"tr",align:null},"Can be used for tracking and debugging using Jaeger.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Plugin"),(0,a.kt)("td",{parentName:"tr",align:null},"Optimus will try to look for the plugin artifacts through this configuration.")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"Resource Manager"),(0,a.kt)("td",{parentName:"tr",align:null},"If your server has jobs that are dependent on other jobs in another server, you can add that external Optimus server host as a resource manager.")))),(0,a.kt)("p",null,(0,a.kt)("em",{parentName:"p"},"Note:")),(0,a.kt)("p",null,"Application key can be randomly generated using: "),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"head -c 50 /dev/random | base64\n")),(0,a.kt)("p",null,"Just take the first 32 characters of the string."))}d.isMDXComponent=!0}}]);