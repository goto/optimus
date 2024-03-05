"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[7326],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>f});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function c(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):c(c({},t),e)),n},u=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},l="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),l=p(n),m=o,f=l["".concat(s,".").concat(m)]||l[m]||d[m]||a;return n?r.createElement(f,c(c({ref:t},u),{},{components:n})):r.createElement(f,c({ref:t},u))}));function f(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,c=new Array(a);c[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[l]="string"==typeof e?e:o,c[1]=i;for(var p=2;p<a;p++)c[p]=n[p];return r.createElement.apply(null,c)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},1586:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>c,default:()=>d,frontMatter:()=>a,metadata:()=>i,toc:()=>p});var r=n(7462),o=(n(7294),n(3905));const a={},c="Job Run",i={unversionedId:"concepts/job-run",id:"concepts/job-run",title:"Job Run",description:"A job is mainly created to run on schedule. Let\u2019s take a look at what is happening once a scheduler triggers your job to run.",source:"@site/docs/concepts/job-run.md",sourceDirName:"concepts",slug:"/concepts/job-run",permalink:"/optimus/docs/concepts/job-run",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/job-run.md",tags:[],version:"current",lastUpdatedBy:"Oky Setiawan",lastUpdatedAt:1709614628,formattedLastUpdatedAt:"Mar 5, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Job",permalink:"/optimus/docs/concepts/job"},next:{title:"Dependency",permalink:"/optimus/docs/concepts/dependency"}},s={},p=[],u={toc:p},l="wrapper";function d(e){let{components:t,...a}=e;return(0,o.kt)(l,(0,r.Z)({},u,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"job-run"},"Job Run"),(0,o.kt)("p",null,"A job is mainly created to run on schedule. Let\u2019s take a look at what is happening once a scheduler triggers your job to run. "),(0,o.kt)("p",null,(0,o.kt)("img",{alt:"Job Run Flow",src:n(9473).Z,title:"JobRunFlow",width:"1600",height:"1131"})),(0,o.kt)("p",null,"Note: to test this runtime interactions on your own, take a look and follow the guide on developer environment ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/goto/optimus/tree/main/dev"},"section"),"."))}d.isMDXComponent=!0},9473:(e,t,n)=>{n.d(t,{Z:()=>r});const r=n.p+"assets/images/Concept_JobRun-de8ca16bc3f20f2d6ae96ae1896e3798.png"}}]);