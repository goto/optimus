"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[6986],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>y});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function c(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):c(c({},t),e)),n},d=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},l="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,d=i(e,["components","mdxType","originalType","parentName"]),l=p(n),m=o,y=l["".concat(s,".").concat(m)]||l[m]||u[m]||a;return n?r.createElement(y,c(c({ref:t},d),{},{components:n})):r.createElement(y,c({ref:t},d))}));function y(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,c=new Array(a);c[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[l]="string"==typeof e?e:o,c[1]=i;for(var p=2;p<a;p++)c[p]=n[p];return r.createElement.apply(null,c)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},8064:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>c,default:()=>u,frontMatter:()=>a,metadata:()=>i,toc:()=>p});var r=n(7462),o=(n(7294),n(3905));const a={},c="Dependency",i={unversionedId:"concepts/dependency",id:"concepts/dependency",title:"Dependency",description:"A job can have a source and a destination to start with. This source could be a resource managed by Optimus or",source:"@site/docs/concepts/dependency.md",sourceDirName:"concepts",slug:"/concepts/dependency",permalink:"/optimus/docs/concepts/dependency",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/dependency.md",tags:[],version:"current",lastUpdatedBy:"Dery Rahman Ahaddienata",lastUpdatedAt:1697527355,formattedLastUpdatedAt:"Oct 17, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Job Run",permalink:"/optimus/docs/concepts/job-run"},next:{title:"Macros",permalink:"/optimus/docs/concepts/macros"}},s={},p=[],d={toc:p},l="wrapper";function u(e){let{components:t,...n}=e;return(0,o.kt)(l,(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"dependency"},"Dependency"),(0,o.kt)("p",null,"A job can have a source and a destination to start with. This source could be a resource managed by Optimus or\nnon-managed like an S3 bucket. If the dependency is managed by Optimus, it is obvious that in an ETL pipeline, it is\nrequired for the dependency to finish successfully first before the dependent job can start."),(0,o.kt)("p",null,"There are 2 types of dependency depending on how to configure it:"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Type"),(0,o.kt)("th",{parentName:"tr",align:null},"Description"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Inferred"),(0,o.kt)("td",{parentName:"tr",align:null},"Automatically detected through assets. The logic on how to detect the dependency is configured in each of the ",(0,o.kt)("a",{parentName:"td",href:"/optimus/docs/concepts/plugin"},"plugins"),".")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"Static"),(0,o.kt)("td",{parentName:"tr",align:null},"Configured through job.yaml")))),(0,o.kt)("p",null,"Optimus also supports job dependency to cross-optimus servers. These Optimus servers are considered external resource\nmanagers, where Optimus will look for the job sources that have not been resolved internally and create the dependency.\nThese resource managers should be configured in the server configuration."))}u.isMDXComponent=!0}}]);