"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2147],{3905:(e,t,r)=>{r.d(t,{Zo:()=>l,kt:()=>f});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function c(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function s(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?c(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):c(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function a(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},c=Object.keys(e);for(n=0;n<c.length;n++)r=c[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var c=Object.getOwnPropertySymbols(e);for(n=0;n<c.length;n++)r=c[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var i=n.createContext({}),p=function(e){var t=n.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):s(s({},t),e)),r},l=function(e){var t=p(e.components);return n.createElement(i.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,c=e.originalType,i=e.parentName,l=a(e,["components","mdxType","originalType","parentName"]),u=p(r),m=o,f=u["".concat(i,".").concat(m)]||u[m]||d[m]||c;return r?n.createElement(f,s(s({ref:t},l),{},{components:r})):n.createElement(f,s({ref:t},l))}));function f(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var c=r.length,s=new Array(c);s[0]=m;var a={};for(var i in t)hasOwnProperty.call(t,i)&&(a[i]=t[i]);a.originalType=e,a[u]="string"==typeof e?e:o,s[1]=a;for(var p=2;p<c;p++)s[p]=r[p];return n.createElement.apply(null,s)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},1317:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>i,contentTitle:()=>s,default:()=>d,frontMatter:()=>c,metadata:()=>a,toc:()=>p});var n=r(7462),o=(r(7294),r(3905));const c={},s="Secret",a={unversionedId:"concepts/secret",id:"concepts/secret",title:"Secret",description:"A lot of transformation operations require credentials to execute. These credentials (secrets) are needed in some tasks,",source:"@site/docs/concepts/secret.md",sourceDirName:"concepts",slug:"/concepts/secret",permalink:"/optimus/docs/concepts/secret",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/secret.md",tags:[],version:"current",lastUpdatedBy:"Sandeep Bhardwaj",lastUpdatedAt:1695718579,formattedLastUpdatedAt:"Sep 26, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Intervals and Windows",permalink:"/optimus/docs/concepts/intervals-and-windows"},next:{title:"Plugin",permalink:"/optimus/docs/concepts/plugin"}},i={},p=[],l={toc:p},u="wrapper";function d(e){let{components:t,...r}=e;return(0,o.kt)(u,(0,n.Z)({},l,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"secret"},"Secret"),(0,o.kt)("p",null,"A lot of transformation operations require credentials to execute. These credentials (secrets) are needed in some tasks,\nhooks, and may also be needed in deployment processes such as dependency resolution. Optimus provides a convenient way\nto store secrets and make them accessible in containers during the execution."),(0,o.kt)("p",null,"You can easily create, update, and delete your own secrets using CLI or REST API. Secrets can be created at a project\nlevel which is accessible from all the namespaces in the project, or can just be created at the namespace level. These\nsecrets will then can be used as part of the job spec configuration using macros with their names. Only the secrets\ncreated at the project & namespace the job belongs to can be referenced."))}d.isMDXComponent=!0}}]);