"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6223],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=r.createContext({}),s=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=s(e.components);return r.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},f=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),u=s(n),f=a,m=u["".concat(l,".").concat(f)]||u[f]||d[f]||o;return n?r.createElement(m,i(i({ref:t},p),{},{components:n})):r.createElement(m,i({ref:t},p))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=f;var c={};for(var l in t)hasOwnProperty.call(t,l)&&(c[l]=t[l]);c.originalType=e,c[u]="string"==typeof e?e:a,i[1]=c;for(var s=2;s<o;s++)i[s]=n[s];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}f.displayName="MDXCreateElement"},9263:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>c,toc:()=>s});var r=n(7462),a=(n(7294),n(3905));const o={},i="Replay & Backup",c={unversionedId:"concepts/replay-and-backup",id:"concepts/replay-and-backup",title:"Replay & Backup",description:"A job might need to be re-run (backfill) due to business requirement changes or other various reasons. Optimus provides",source:"@site/docs/concepts/replay-and-backup.md",sourceDirName:"concepts",slug:"/concepts/replay-and-backup",permalink:"/optimus/docs/concepts/replay-and-backup",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/replay-and-backup.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1701854414,formattedLastUpdatedAt:"Dec 6, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Plugin",permalink:"/optimus/docs/concepts/plugin"},next:{title:"Server Configuration",permalink:"/optimus/docs/server-guide/configuration"}},l={},s=[],p={toc:s},u="wrapper";function d(e){let{components:t,...o}=e;return(0,a.kt)(u,(0,r.Z)({},p,o,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"replay--backup"},"Replay & Backup"),(0,a.kt)("p",null,"A job might need to be re-run (backfill) due to business requirement changes or other various reasons. Optimus provides\nan easy way to do this using Replay. Replay accepts which job and range of date to be updated, validates it, and re-runs\nthe job tasks."),(0,a.kt)("p",null,"When validating, Optimus checks if there is any Replay with the same job and date currently running and also checks if\nthe task scheduler instances are still running to avoid any duplication and conflicts."),(0,a.kt)("p",null,"After passing the validation checks, a Replay request will be created and will be processed by the workers based on the\nmode chosen (sequential/parallel). To re-run the tasks, Optimus clears the existing runs from the scheduler."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Sequential (Default)")),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"Sequential Mode Flow",src:n(9800).Z,title:"SequentialMode",width:"922",height:"222"})),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Parallel")),(0,a.kt)("p",null,(0,a.kt)("img",{alt:"Parallel Mode Flow",src:n(4097).Z,title:"ParallelMode",width:"814",height:"191"})),(0,a.kt)("p",null,"Optimus also provides a Backup feature to duplicate a resource that can be perfectly used before running Replay. Where\nthe backup result will be located, and the expiry detail can be configured in the project configuration."))}d.isMDXComponent=!0},4097:(e,t,n)=>{n.d(t,{Z:()=>r});const r=n.p+"assets/images/ReplayParallel-380cc531902871dd09bad2213c78b905.png"},9800:(e,t,n)=>{n.d(t,{Z:()=>r});const r=n.p+"assets/images/ReplaySequential-fc2c15aa94e0cdbde7348b508019546d.png"}}]);