"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9439],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>f});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function c(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),i=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):c(c({},t),e)),r},p=function(e){var t=i(e.components);return n.createElement(s.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=i(r),m=a,f=u["".concat(s,".").concat(m)]||u[m]||d[m]||o;return r?n.createElement(f,c(c({ref:t},p),{},{components:r})):n.createElement(f,c({ref:t},p))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,c=new Array(o);c[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:a,c[1]=l;for(var i=2;i<o;i++)c[i]=r[i];return n.createElement.apply(null,c)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},2272:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>c,default:()=>d,frontMatter:()=>o,metadata:()=>l,toc:()=>i});var n=r(7462),a=(r(7294),r(3905));const o={},c="Macros",l={unversionedId:"concepts/macros",id:"concepts/macros",title:"Macros",description:"Macros are special variables that will be replaced by actual values when before execution. Custom macros are not",source:"@site/docs/concepts/macros.md",sourceDirName:"concepts",slug:"/concepts/macros",permalink:"/optimus/docs/concepts/macros",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/macros.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1693558284,formattedLastUpdatedAt:"Sep 1, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Dependency",permalink:"/optimus/docs/concepts/dependency"},next:{title:"Intervals and Windows",permalink:"/optimus/docs/concepts/intervals-and-windows"}},s={},i=[],p={toc:i},u="wrapper";function d(e){let{components:t,...r}=e;return(0,a.kt)(u,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"macros"},"Macros"),(0,a.kt)("p",null,"Macros are special variables that will be replaced by actual values when before execution. Custom macros are not\nsupported yet."),(0,a.kt)("p",null,"Below listed the in built macros supported in optimus."),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Macros"),(0,a.kt)("th",{parentName:"tr",align:null},"Description"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"{{.DSTART}}"),(0,a.kt)("td",{parentName:"tr",align:null},"start date/datetime of the window as 2021-02-10T10:00:00+00:00 that is, RFC3339")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"{{.DEND}}"),(0,a.kt)("td",{parentName:"tr",align:null},"end date/datetime of the window, as RFC3339")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"{{.JOB_DESTINATION}}"),(0,a.kt)("td",{parentName:"tr",align:null},"full qualified table name used in DML statement")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"{{.EXECUTION_TIME}}"),(0,a.kt)("td",{parentName:"tr",align:null},"timestamp when the specific job run starts")))),(0,a.kt)("p",null,"Take a detailed look at the windows concept and example ",(0,a.kt)("a",{parentName:"p",href:"/optimus/docs/concepts/intervals-and-windows"},"here"),"."))}d.isMDXComponent=!0}}]);