"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6223],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>m});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),s=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=s(e.components);return n.createElement(c.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,c=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=s(r),f=a,m=u["".concat(c,".").concat(f)]||u[f]||d[f]||o;return r?n.createElement(m,i(i({ref:t},p),{},{components:r})):n.createElement(m,i({ref:t},p))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=f;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l[u]="string"==typeof e?e:a,i[1]=l;for(var s=2;s<o;s++)i[s]=r[s];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},9263:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var n=r(7462),a=(r(7294),r(3905));const o={},i="Replay & Backup",l={unversionedId:"concepts/replay-and-backup",id:"concepts/replay-and-backup",title:"Replay & Backup",description:"A job might need to be re-run (backfill) due to business requirement changes or other various reasons. Optimus provides",source:"@site/docs/concepts/replay-and-backup.md",sourceDirName:"concepts",slug:"/concepts/replay-and-backup",permalink:"/optimus/docs/concepts/replay-and-backup",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/replay-and-backup.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1683800037,formattedLastUpdatedAt:"May 11, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Plugin",permalink:"/optimus/docs/concepts/plugin"},next:{title:"Server Configuration",permalink:"/optimus/docs/server-guide/configuration"}},c={},s=[],p={toc:s},u="wrapper";function d(e){let{components:t,...o}=e;return(0,a.kt)(u,(0,n.Z)({},p,o,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"replay--backup"},"Replay & Backup"),(0,a.kt)("p",null,"A job might need to be re-run (backfill) due to business requirement changes or other various reasons. Optimus provides\nan easy way to do this using Replay. Replay accepts which job and range of date to be updated, validates it, and re-runs\nthe job tasks."),(0,a.kt)("p",null,"When validating, Optimus checks if there is any Replay with the same job and date currently running and also checks if\nthe task scheduler instances are still running to avoid any duplication and conflicts."),(0,a.kt)("p",null,"After passing the validation checks, a Replay request will be created and will be processed by the workers based on the\nmode chosen (sequential/parallel). To re-run the tasks, Optimus clears the existing runs from the scheduler."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Sequential (Default)")),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"Sequential Mode Flow",src:r(9800).Z,title:"SequentialMode",width:"922",height:"222"})),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Parallel")),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"Parallel Mode Flow",src:r(4097).Z,title:"ParallelMode",width:"814",height:"191"})),(0,a.kt)("p",null,"Optimus also provides a Backup feature to duplicate a resource that can be perfectly used before running Replay. Where\nthe backup result will be located, and the expiry detail can be configured in the project configuration."))}d.isMDXComponent=!0},4097:(e,t,r)=>{r.d(t,{Z:()=>n});const n=r.p+"assets/images/ReplayParallel-380cc531902871dd09bad2213c78b905.png"},9800:(e,t,r)=>{r.d(t,{Z:()=>n});const n=r.p+"assets/images/ReplaySequential-fc2c15aa94e0cdbde7348b508019546d.png"}}]);