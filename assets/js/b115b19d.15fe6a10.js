"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[9546],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>f});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function s(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function c(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var i=n.createContext({}),p=function(e){var t=n.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):s(s({},t),e)),r},u=function(e){var t=p(e.components);return n.createElement(i.Provider,{value:t},e.children)},l="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,i=e.parentName,u=c(e,["components","mdxType","originalType","parentName"]),l=p(r),m=o,f=l["".concat(i,".").concat(m)]||l[m]||d[m]||a;return r?n.createElement(f,s(s({ref:t},u),{},{components:r})):n.createElement(f,s({ref:t},u))}));function f(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,s=new Array(a);s[0]=m;var c={};for(var i in t)hasOwnProperty.call(t,i)&&(c[i]=t[i]);c.originalType=e,c[l]="string"==typeof e?e:o,s[1]=c;for(var p=2;p<a;p++)s[p]=r[p];return n.createElement.apply(null,s)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},126:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>i,contentTitle:()=>s,default:()=>d,frontMatter:()=>a,metadata:()=>c,toc:()=>p});var n=r(7462),o=(r(7294),r(3905));const a={},s="Resource",c={unversionedId:"concepts/resource",id:"concepts/resource",title:"Resource",description:"A resource is the representation of the warehouse unit that can be a source or a destination of a transformation job.",source:"@site/docs/concepts/resource.md",sourceDirName:"concepts",slug:"/concepts/resource",permalink:"/optimus/docs/concepts/resource",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/concepts/resource.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1681734362,formattedLastUpdatedAt:"Apr 17, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Namespace",permalink:"/optimus/docs/concepts/namespace"},next:{title:"Job",permalink:"/optimus/docs/concepts/job"}},i={},p=[],u={toc:p},l="wrapper";function d(e){let{components:t,...r}=e;return(0,o.kt)(l,(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"resource"},"Resource"),(0,o.kt)("p",null,"A resource is the representation of the warehouse unit that can be a source or a destination of a transformation job.\nThe warehouse resources can be created, modified, and be read from Optimus, as well as can be backed up as requested.\nEach warehouse supports a fixed set of resource types and each type has its own specification schema.\nOptimus\u2019 managed warehouse  is called Optimus datastore."),(0,o.kt)("p",null,"At the moment, Optimus supports BigQuery datastore for these type of resources:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Dataset"),(0,o.kt)("li",{parentName:"ul"},"Table"),(0,o.kt)("li",{parentName:"ul"},"Standard View"),(0,o.kt)("li",{parentName:"ul"},"External Table")),(0,o.kt)("p",null,(0,o.kt)("em",{parentName:"p"},"Note: BigQuery resource deletion is currently not supported.")))}d.isMDXComponent=!0}}]);