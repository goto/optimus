"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[8124],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>b});var i=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,i,r=function(e,t){if(null==e)return{};var n,i,r={},a=Object.keys(e);for(i=0;i<a.length;i++)n=a[i],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(i=0;i<a.length;i++)n=a[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=i.createContext({}),c=function(e){var t=i.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=c(e.components);return i.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},m=i.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=c(n),m=r,b=u["".concat(l,".").concat(m)]||u[m]||d[m]||a;return n?i.createElement(b,o(o({ref:t},p),{},{components:n})):i.createElement(b,o({ref:t},p))}));function b(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,o=new Array(a);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[u]="string"==typeof e?e:r,o[1]=s;for(var c=2;c<a;c++)o[c]=n[c];return i.createElement.apply(null,o)}return i.createElement.apply(null,n)}m.displayName="MDXCreateElement"},7470:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>a,metadata:()=>s,toc:()=>c});var i=n(7462),r=(n(7294),n(3905));const a={},o="Verifying the Jobs",s={unversionedId:"client-guide/verifying-jobs",id:"client-guide/verifying-jobs",title:"Verifying the Jobs",description:"Minimize the chances of having the job failed in runtime by validating and inspecting it before deployment.",source:"@site/docs/client-guide/verifying-jobs.md",sourceDirName:"client-guide",slug:"/client-guide/verifying-jobs",permalink:"/optimus/docs/client-guide/verifying-jobs",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/verifying-jobs.md",tags:[],version:"current",lastUpdatedBy:"Arinda Arif",lastUpdatedAt:1710936479,formattedLastUpdatedAt:"Mar 20, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Setting up Alert to Job",permalink:"/optimus/docs/client-guide/setting-up-alert"},next:{title:"Applying Job Specifications",permalink:"/optimus/docs/client-guide/applying-job-specifications"}},l={},c=[{value:"Validate Jobs",id:"validate-jobs",level:2},{value:"Inspect Job",id:"inspect-job",level:2}],p={toc:c},u="wrapper";function d(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,i.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"verifying-the-jobs"},"Verifying the Jobs"),(0,r.kt)("p",null,"Minimize the chances of having the job failed in runtime by validating and inspecting it before deployment."),(0,r.kt)("h2",{id:"validate-jobs"},"Validate Jobs"),(0,r.kt)("p",null,"Job validation is being done per namespace. Try it out by running this command:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job validate --namespace sample_namespace --verbose\n")),(0,r.kt)("p",null,"Make sure you are running the above command in the same directory as where your client configuration (optimus.yaml)\nis located. Or if not, you can provide the command by adding a config flag."),(0,r.kt)("p",null,"By running the above command, Optimus CLI will try to fetch all of the jobs under sample_namespace\u2019s job path that\nhas been specified in the client configuration. The verbose flag will be helpful to print out the jobs being processed.\nAny jobs that have missing mandatory configuration, contain an invalid query, or cause cyclic dependency will be pointed out."),(0,r.kt)("h2",{id:"inspect-job"},"Inspect Job"),(0,r.kt)("p",null,"You can try to inspect a single job, for example checking what are the upstream/dependencies, does it has any downstream,\nor whether it has any warnings. This inspect command can be done against a job that has been registered or not registered\nto your Optimus server."),(0,r.kt)("p",null,"To inspect a job in your local:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job inspect <job_name>\n")),(0,r.kt)("p",null,"To inspect a job in the server:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus job inspect <job_name> --server\n")),(0,r.kt)("p",null,"You will find mainly 3 sections for inspection:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Basic Info"),":\nOptimus will print the job\u2019s specification, including the resource destination & sources (if any), and whether it has any soft warning."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Upstreams"),":\nWill prints what are the jobs that this job depends on. Do notice there might be internal upstreams, external (cross-server) upstreams, HTTP upstreams, and unknown upstreams (not registered in Optimus)."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Downstreams"),":\nWill prints what are the jobs that depends on this job.")))}d.isMDXComponent=!0}}]);